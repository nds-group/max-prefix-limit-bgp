# %% [markdown]
# # Compute Prefix Visibility Across RRCs (Per-Datetime Processing)
#
# This notebook processes RIB files and saves individual per-datetime visibility files.
#
# **Features:**
# - Saves one file per timestamp (`visibility_{YYYYMMDD_HHMM}.pkl`)
# - Skips already-processed dates (resume capability)
# - Fault-tolerant: can be interrupted and resumed
# - Script-ready: designed for background execution
#
# **Usage:**
# - Run all cells for notebook execution
# - Or convert to script: `jupyter nbconvert --to script notebook.ipynb`
# - Monitor progress: `python ../scripts/monitor_visibility_progress.py`

# %% [markdown]
# ## Imports

import datetime
import json

# %%
import os
import pickle
import re
from collections import defaultdict
from multiprocessing import Pool

# %% [markdown]
# ## Configuration

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
    total_rrcs = len(collectors)

    # Output directory for per-datetime files
    output_dir = parameters.get(
        "VISIBILITY_OUTPUT_DIR", f"{data_dir}/processed/prefix_visibility/"
    )
except:
    raise ValueError("Invalid parameter file")

# Create output directory
os.makedirs(output_dir, exist_ok=True)

print(f"Configuration loaded:")
print(f"  Date range: {start_date.date()} to {end_date.date()}")
print(f"  Total RRCs: {total_rrcs}")
print(f"  Processes: {n_processes}")
print(f"  Output dir: {output_dir}")

# %% [markdown]
# ## Build Files Dictionary

# %%
delta = datetime.timedelta(hours=8)
all_times = []
current = start_date
while current < end_date:
    all_times.append(current)
    current += delta

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
print(f"Found {len(files_times)} time points to process")
print(f"Example: {files_times[0][0]} -> {len(files_times[0][1])} files")

# %% [markdown]
# ## Bogus ASN Classification Function


# %%
def is_bogon_asn(asn: int) -> bool:
    """Check whether the given ASN is a bogon (reserved/invalid)."""
    if asn == 0:  # RFC7607
        return True
    if asn == 23456:  # RFC4893 AS_TRANS
        return True
    if 64496 <= asn <= 64511:  # RFC5398 documentation/example
        return True
    if 65536 <= asn <= 65551:  # RFC5398 documentation/example (32-bit)
        return True
    if 64512 <= asn <= 65534:  # RFC6996 private ASNs (16-bit)
        return True
    if 4200000000 <= asn <= 4294967294:  # RFC6996 private ASNs (32-bit)
        return True
    if asn == 65535 or asn == 4294967295:  # RFC7300 last 16/32-bit ASNs
        return True
    if 65552 <= asn <= 131071:  # IANA reserved
        return True
    return False


# %% [markdown]
# ## Prefix Visibility Computation with File Saving
#
# This function:
# 1. Checks if output file exists (skip if already processed)
# 2. Processes the timestamp
# 3. Immediately saves results to disk
# 4. Returns summary stats only


# %%
def extract_rrc_from_path(filepath):
    """Extract RRC name (e.g., 'rrc00') from file path."""
    match = re.search(r"/(rrc\d+)/", filepath)
    if match:
        return match.group(1)
    return None


def compute_prefix_visibility(file_time):
    """Compute prefix visibility and save to individual file."""
    time, files_collectors = file_time

    # Generate output filename
    time_str = time.strftime("%Y%m%d_%H%M")
    output_file = f"{output_dir}/visibility_{time_str}.pkl"

    # Skip if already processed
    if os.path.exists(output_file):
        print(f"⏭️  Skipping {time} (already processed)")
        return None

    print(f"\n🔄 Processing time {time}...")

    # Track prefix -> RRC mapping
    prefix_rrc_tracking = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    all_asns = set()

    # Load all RIB files
    for file in files_collectors:
        rrc_name = extract_rrc_from_path(file)
        if not rrc_name:
            continue

        try:
            with open(file, "rb") as fd:
                data = pickle.load(fd)
                prefixes_data = data["prefixes"]

                for asn in prefixes_data:
                    all_asns.add(asn)
                    for ip_version in prefixes_data[asn]:
                        for prefix in prefixes_data[asn][ip_version]:
                            prefix_rrc_tracking[asn][ip_version][prefix].add(rrc_name)
        except:
            continue

    # Build visibility data
    visibility_data = {}
    for asn in all_asns:
        visibility_data[asn] = {"bogus": is_bogon_asn(asn)}
        if visibility_data[asn]["bogus"]:
            continue

        for ip_version in prefix_rrc_tracking[asn]:
            prefixes_dict = {}
            for prefix, rrc_set in prefix_rrc_tracking[asn][ip_version].items():
                visibility_pct = (len(rrc_set) / total_rrcs) * 100
                prefixes_dict[prefix] = {
                    "rrc_set": rrc_set,
                    "visibility": round(visibility_pct, 2),
                }
            visibility_data[asn][ip_version] = {
                "n_total_prefixes": len(prefixes_dict),
                "prefixes": prefixes_dict,
            }

    # Calculate stats
    bogus_count = sum(1 for asn in all_asns if is_bogon_asn(asn))
    stats = {
        "total_asns": len(all_asns),
        "bogus_asns": bogus_count,
        "valid_asns": len(all_asns) - bogus_count,
    }

    # Save to file
    result = {"timestamp": time, "asn_data": visibility_data, "stats": stats}
    try:
        with open(output_file, "wb") as f:
            pickle.dump(result, f)
        print(f"✓ Saved {output_file}")
        print(f"  ASNs: {stats['total_asns']} (Bogus: {stats['bogus_asns']})")
    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    return stats


# %% [markdown]
# ## Parallel Processing
#
# Process all timestamps in parallel.

# %%
# For testing:
# files_times_subset = files_times[:5]
files_times_subset = files_times

print(f"Starting parallel processing of {len(files_times_subset)} timestamps...")
print(f"Output directory: {output_dir}")
print(f"\nTip: Monitor with: python ../scripts/monitor_visibility_progress.py\n")

if __name__ == "__main__":
    with Pool(n_processes) as p:
        results = p.map(compute_prefix_visibility, files_times_subset)

    completed = [r for r in results if r is not None]
    skipped = len(results) - len(completed)

    print(f"\n" + "=" * 60)
    print(f"PROCESSING COMPLETE")
    print(f"=" * 60)
    print(f"  Total: {len(results)}")
    print(f"  Processed: {len(completed)}")
    print(f"  Skipped: {skipped}")
    print(f"  Output: {output_dir}")
    print(f"\n✓ All files saved!")

# %% [markdown]
# ## Processing Summary

# %%
if completed:
    print("\nProcessing Statistics:")
    print(f"  Timestamps processed: {len(completed)}")
    print(f"  Total ASN entries: {sum(r['total_asns'] for r in completed)}")
    print(f"  Valid ASN entries: {sum(r['valid_asns'] for r in completed)}")
    print(f"  Bogus ASN entries: {sum(r['bogus_asns'] for r in completed)}")
else:
    print("No new timestamps processed (all already complete)")
