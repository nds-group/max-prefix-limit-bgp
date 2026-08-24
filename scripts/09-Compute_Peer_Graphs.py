# %% [markdown]
# # Compute number of peers
#
# This notebook compute the numbers of peers per AS, by using the graphs obtained previously
#
# - The input is a set of graphs
# - The output is a pandas dataframe with a timeseries per AS and IP version of the number of peers at a given moment

import datetime
import json

# %%
import os
import pickle
from multiprocessing import Pool

import networkx as nx
import pandas as pd
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

files_times = {}
for time_value in all_times:
    time_str = time_value.strftime("%Y%m%d.%H%M")
    year_month = time_value.strftime("%Y.%m")

    if time_value not in files_times:
        files_times[time_value] = []

    for collector in collectors:
        file = f"{data_dir}/processed/RIBs/RIPE/{collector}/{year_month}/prefix_peers_{time_str}.pkl"
        if os.path.exists(file):
            files_times[time_value].append(file)

files_times = list(files_times.items())

# %%

# %% [markdown]
# ## Extract Peers


# %%
def get_peers_graph(file_time):
    time, files = file_time

    print(f"\nProcessing time {time}...\n")

    G_ipv4 = nx.Graph()
    G_ipv6 = nx.Graph()

    for file in files:

        try:
            with open(file, "rb") as fd:
                data = pickle.load(fd)

                G_ipv4_collector = data["peers_ipv4"].to_undirected()
                G_ipv6_collector = data["peers_ipv6"].to_undirected()

                G_ipv4 = nx.compose(G_ipv4, G_ipv4_collector)
                G_ipv6 = nx.compose(G_ipv6, G_ipv6_collector)
        except Exception as e:
            print(f"Error processing file {file}: {e}")
            continue

    data = {"time": time, "G_ipv4": G_ipv4, "G_ipv6": G_ipv6}

    time_str = time.strftime("%Y%m%d.%H%M")
    filename = f"{data_dir}/processed/peers/graphs/peers_graph_{time_str}.pkl"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "wb") as fd:
        pickle.dump(data, fd)


# %%
if __name__ == "__main__":
    actual_processes = min(n_processes, len(files_times))
    with Pool(actual_processes) as p:
        graphs = p.map(get_peers_graph, files_times)
