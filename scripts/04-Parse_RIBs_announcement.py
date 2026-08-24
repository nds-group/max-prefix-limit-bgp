# %% [markdown]
# # Compute prefixes being announced
#
# In this notebook, we compute the prefixes that the ASes are announcing with the following methodology
# - Find the paths with a tier 1 AS: [AS1, AS2, ..., AS_TIER1, ..., AS_N]
# - Assigning the prefix as a prefix announced for all the ASes in the set {AS_TIER1, ..., AS_N}
#
# - The input is a raw RIBs file
# - The output is a dictionary with the following structure announce_prefix[downstream_asn][ip_version] = {set of prefixes}

import datetime
import json

# %%
import os
import pickle
from multiprocessing import Pool

import networkx as nx
import pandas as pd
from pybgpkit_parser import Parser
from sortedcontainers import SortedDict, SortedSet
from tqdm import tqdm

# %% [markdown]
# ## Confs

# %%
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
fd = open(os.path.join(REPO_ROOT, "settings.json"))
parameters = json.load(fd)
for _k in ("DATA_DIR", "DATA_RAW_DIR", "IMAGE_DIR", "WORKING_DIR", "VISIBILITY_OUTPUT_DIR", "VISIBILITY_ANNOUNCED_OUTPUT_DIR"):
    if isinstance(parameters.get(_k), str) and not os.path.isabs(parameters[_k]):
        parameters[_k] = os.path.normpath(os.path.join(REPO_ROOT, parameters[_k]))
fd.close()

try:
    data_dir = parameters["DATA_DIR"]
    data_raw_dir = parameters["DATA_RAW_DIR"]
    start_date = parameters["START_DATE"]
    end_date = parameters["END_DATE"]
    img_dir = parameters["IMAGE_DIR"]
    n_processes = parameters["N_PROCESSES"]
    collectors = parameters["COLLECTORS"]
    tier_1 = parameters["TIER1"]

    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    n_processes = int(n_processes)
except:
    raise ValueError("Invalid parameter file")

# %% [markdown]
# ## Load Data

# %%
delta = datetime.timedelta(hours=8)
all_times = []
while start_date < end_date:
    all_times.append(start_date)
    start_date += delta

files_times = []
for time_value in all_times:
    time_str = time_value.strftime("%Y%m%d.%H%M")
    year_month = time_value.strftime("%Y.%m")

    for collector in collectors:
        file = f"{data_dir}/raw/RIBs/RIPE/{collector}/{year_month}/bview.{time_str}.gz"
        if os.path.exists(file):
            files_times.append((time_value, collector, file))

# %%
tier_1_asn = [tier_1[i]["asn"] for i in range(len(tier_1))]
tier_1_asn = SortedSet(tier_1_asn)

tier_1_regex = "|".join(map(str, tier_1_asn))
tier_1_regex = rf"\b({tier_1_regex})\b"
print("Regular expression to match paths with Tier 1 ASNs:")
print(tier_1_regex)

# %% [markdown]
# ## Process Data


# %%
def process_file(file_time):

    time_value, collector, file = file_time

    year_month = time_value.strftime("%Y.%m")
    timestamp_str = time_value.strftime("%Y%m%d.%H%M")

    output_folder = f"{data_dir}/processed/RIBS_announce/RIPE/{collector}/{year_month}"
    output_file = f"{output_folder}/prefix_peers_{timestamp_str}.pkl"

    if os.path.exists(output_file):
        print(f"\nSkipping {output_file} already exists. ✅\n")
        return

    os.makedirs(output_folder, exist_ok=True)
    print(f"\nProcessing file: {file} 💻...\n")

    announce_prefix = SortedDict()

    parser = Parser(file, filters={"as_path": tier_1_regex})  # paths with a tier1 AS
    for elem in parser:

        origins = elem.origin_asns
        prefix = elem.prefix
        as_path = elem.as_path

        if "{" in origins or "{" in as_path:
            continue  # Skip elements with set notation in origins or as_path

        try:
            as_path = [int(asn) for asn in as_path.split()]
        except:
            os.system(
                f'echo "Error parsing origins {origins} with as_path {as_path} in file {file}" >> errors_2.log'
            )
            continue

        if len(as_path) < 2:
            continue

        ip_version = 6 if ":" in prefix else 4

        for i in range(len(as_path) - 1, -1, -1):
            if as_path[i] in tier_1_asn:
                downstream_asns = as_path[
                    i:
                ]  # include the Tier 1 AS in the announcement
                for downstream_asn in set(
                    downstream_asns
                ):  # set this prefix as an announcement for all the ASes before the Tier1
                    if downstream_asn not in announce_prefix:
                        announce_prefix[downstream_asn] = SortedDict()
                    if ip_version not in announce_prefix[downstream_asn]:
                        announce_prefix[downstream_asn][ip_version] = SortedSet()

                    announce_prefix[downstream_asn][ip_version].add(prefix)
                break
    # save data
    full_data = {
        "datetime": time_value,
        "announce_prefixes": announce_prefix,
    }

    fd = open(output_file, "wb")
    pickle.dump(full_data, fd)
    fd.close()

    del announce_prefix, full_data, parser
    print(f"\nFile {file} processed and saved to {output_folder} ✅\n")


# %%
if __name__ == "__main__":
    with Pool(n_processes) as p:
        p.map(process_file, files_times)

# %%
