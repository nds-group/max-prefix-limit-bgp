# %% [markdown]
# # Extract Peers and Prefixes from RIBs
#
# This notebook parse using BGPKit the RIBs files and extract the peers and prefixes:
# - The input is RIBs raw files
# - The output is object with two graphs for IPv4 and IPv6 to represent the peer relationships, and a dictionary origin_prefix[origin_asn][ip_version] with all the prefixes being originated from the ASes

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
    data_raw_files = parameters["DATA_RAW_DIR"]
    start_date = parameters["START_DATE"]
    end_date = parameters["END_DATE"]
    img_dir = parameters["IMAGE_DIR"]
    n_processes = parameters["N_PROCESSES"]
    collectors = parameters["COLLECTORS"]

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
        file = (
            f"{data_raw_files}/RIBs/RIPE/{collector}/{year_month}/bview.{time_str}.gz"
        )
        if os.path.exists(file):
            files_times.append((time_value, collector, file))


# %%
def process_file(file_time):

    time_value, collector, file = file_time

    year_month = time_value.strftime("%Y.%m")
    timestamp_str = time_value.strftime("%Y%m%d.%H%M")

    output_folder = f"{data_dir}/processed/RIBs/RIPE/{collector}/{year_month}"
    output_file = f"{output_folder}/prefix_peers_{timestamp_str}.pkl"

    if os.path.exists(output_file):
        print(f"\nSkipping {output_file} already exists. ✅\n")
        return

    os.makedirs(output_folder, exist_ok=True)
    print(f"\nProcessing file: {file} 💻...\n")

    G_ipv4 = nx.DiGraph()
    G_ipv6 = nx.DiGraph()

    origin_prefix = SortedDict()

    parser = Parser(file)
    for elem in parser:

        origins = elem.origin_asns
        prefix = elem.prefix
        as_path = elem.as_path

        if "{" in origins or "{" in as_path:
            continue  # Skip elements with set notation in origins or as_path

        try:
            origin_asn = int(origins[0])
        except:
            os.system(
                f'echo "Error parsing origins {origins} in file {file}" >> errors.log'
            )
            continue

        ip_version = 6 if ":" in prefix else 4
        if origin_asn not in origin_prefix:
            origin_prefix[origin_asn] = SortedDict()
        if ip_version not in origin_prefix[origin_asn]:
            origin_prefix[origin_asn][ip_version] = SortedSet()

        origin_prefix[origin_asn][ip_version].add(prefix)

        as_path = [int(asn) for asn in as_path.split()]
        if len(as_path) < 2:
            continue

        for i in range(len(as_path) - 1):
            dest = as_path[i]
            src = as_path[i + 1]
            if dest == src:
                continue

            if ip_version == 4:
                G_ipv4.add_edge(src, dest)
            else:
                G_ipv6.add_edge(src, dest)

    # save data
    full_data = {
        "datetime": time_value,
        "peers_ipv4": G_ipv4,
        "peers_ipv6": G_ipv6,
        "prefixes": origin_prefix,
    }

    fd = open(output_file, "wb")
    pickle.dump(full_data, fd)
    fd.close()

    del G_ipv4, G_ipv6, origin_prefix, full_data
    print(f"\nFile {file} processed and saved to {output_folder} ✅\n")


# %%
n_processes = min(n_processes, len(files_times))

if __name__ == "__main__":
    with Pool(n_processes) as p:
        p.map(process_file, files_times)

# %%
