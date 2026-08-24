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
fd = open(os.path.join(REPO_ROOT, "settings.json"), "r")
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

# %% [markdown]
# ## Extract Peers


# %%
def get_peers(file_time):
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

    if len(G_ipv4) > 0:
        ipv4_peers = {node: len(neighbors) for node, neighbors in G_ipv4.adjacency()}
        df_ipv4_peers = pd.DataFrame.from_dict(ipv4_peers, orient="index").reset_index()
        df_ipv4_peers.columns = ["asn", "num_peers"]
        df_ipv4_peers["datetime"] = time
    else:
        df_ipv4_peers = None

    if len(G_ipv6) > 0:
        ipv6_peers = {node: len(neighbors) for node, neighbors in G_ipv6.adjacency()}
        df_ipv6_peers = pd.DataFrame.from_dict(ipv6_peers, orient="index").reset_index()
        df_ipv6_peers.columns = ["asn", "num_peers"]
        df_ipv6_peers["datetime"] = time
    else:
        df_ipv6_peers = None

    return (df_ipv4_peers, df_ipv6_peers)


# %%
if __name__ == "__main__":
    with Pool(n_processes) as p:
        dfs_peers = p.map(get_peers, files_times)

# %% [markdown]
# ### Group by ASN and IP Version (time series)

# %%
dfs_peers_ipv4 = [df_peers[0] for df_peers in dfs_peers if df_peers[0] is not None]
df_peers_ipv4 = pd.concat(dfs_peers_ipv4, ignore_index=True)
dfs_peers_ipv6 = [df_peers[1] for df_peers in dfs_peers if df_peers[1] is not None]
df_peers_ipv6 = pd.concat(dfs_peers_ipv6, ignore_index=True)

df_peers_ipv4 = (
    df_peers_ipv4.groupby(["asn"])
    .agg({"num_peers": lambda x: list(x), "datetime": lambda x: list(x)})
    .reset_index()
)
df_peers_ipv4["ip_version"] = 4

df_peers_ipv6 = (
    df_peers_ipv6.groupby(["asn"])
    .agg({"num_peers": lambda x: list(x), "datetime": lambda x: list(x)})
    .reset_index()
)
df_peers_ipv6["ip_version"] = 6

# %% [markdown]
# ### Example Data

# %%
df_peers_ipv4.head(5)

# %%
df_peers_ipv6.head(5)

# %% [markdown]
# ## Save Data

# %%
filename_peers_ipv4 = f"{data_dir}/processed/peers/df_peers_ipv4.pkl"
df_peers_ipv4.to_pickle(filename_peers_ipv4)

filename_peers_ipv6 = f"{data_dir}/processed/peers/df_peers_ipv6.pkl"
df_peers_ipv6.to_pickle(filename_peers_ipv6)

# %%
