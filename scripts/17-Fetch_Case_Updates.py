# %% [markdown]
# # 11.6 Fetch BGP Updates for the Case Studies
#
# Downloads RIPE RIS 5-minute UPDATE dumps for a +/-N-day window around each of the four
# case-study crossings, storing the raw MRT under `data/raw/updates/RIPE/`. Mirrors
# `0-Download_RIBs.py` but for update files and with `multiprocessing.Pool`.
#
# Long-running -> run as a script in tmux:
#     activate env first, then
#     cd scripts && python 11.6-Fetch_updates_use_cases.py
#
# Pause/resume: safe to Ctrl-C and re-run. Complete files are skipped; downloads land
# atomically (`.part` -> rename) so an interrupted file is never mistaken for complete.
# Inspect disk without downloading:  python 11.6-Fetch_updates_use_cases.py --check
#
# No parsing here (see 11.7 for the zoom plots). Withdrawals carry no AS-path, so we cannot
# filter server-side; we download the full update files for the window and filter later.

import datetime
import json

# %%
import os
import sys
import urllib.request
from multiprocessing import Pool

from tqdm import tqdm

# %% [markdown]
# ## Config

# %%
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
fd = open(os.path.join(REPO_ROOT, "settings.json"))
parameters = json.load(fd)
for _k in ("DATA_DIR", "DATA_RAW_DIR", "IMAGE_DIR", "WORKING_DIR", "VISIBILITY_OUTPUT_DIR", "VISIBILITY_ANNOUNCED_OUTPUT_DIR"):
    if isinstance(parameters.get(_k), str) and not os.path.isabs(parameters[_k]):
        parameters[_k] = os.path.normpath(os.path.join(REPO_ROOT, parameters[_k]))
fd.close()

data_raw_dir = parameters["DATA_RAW_DIR"]
collectors = parameters["COLLECTORS"]
n_processes = int(parameters["N_PROCESSES"])

# knobs (kept explicit so the window is trivial to widen later)
WINDOW_DAYS = 1  # +/- days around each crossing
UPDATE_INTERVAL_MIN = 5  # RIS native update cadence
DOWNLOAD_WORKERS = min(n_processes, 12)  # be courteous to the RIPE archive

updates_root = f"{data_raw_dir}/updates/RIPE"
os.makedirs(updates_root, exist_ok=True)

CHECK_ONLY = "--check" in sys.argv

# %% [markdown]
# ## Case-study crossings (hardcoded; paper Section 6.3)

# %%
CASES = {
    "AS25273_BCE_v4": datetime.date(2025, 9, 5),  # Case 1 (IPv4), rollback ~Sep 8
    "AS52920_IVOCS_v4": datetime.date(2025, 8, 12),  # Case 2 (IPv4)
    "AS44901_BelCloud_v6":  datetime.date(2025, 1, 15),  # Case 3 (IPv6), PeeringDB fix within hours
    "AS52603_SupplyNet_v6": datetime.date(2025, 9, 2),   # Case 4 (IPv6), cycles Oct-Nov (widen window later)
}


def window_timestamps(crossing_date, window_days, interval_min):
    """All 5-min timestamps over [crossing - window_days, crossing + window_days] inclusive."""
    start = datetime.datetime.combine(
        crossing_date - datetime.timedelta(days=window_days), datetime.time.min
    )
    end = datetime.datetime.combine(
        crossing_date + datetime.timedelta(days=window_days), datetime.time.max
    )
    step = datetime.timedelta(minutes=interval_min)
    out, t = [], start
    while t <= end:
        out.append(t)
        t += step
    return out


# %% [markdown]
# ## Build the download task list
#
# One file per (collector, 5-min slot); overlapping case windows de-duplicated.


# %%
def build_tasks():
    tasks = []  # (remote_url, local_path)
    seen = set()  # (collector, yyyymmdd, hhmm)
    for cdate in CASES.values():
        for t in window_timestamps(cdate, WINDOW_DAYS, UPDATE_INTERVAL_MIN):
            ymd = t.strftime("%Y%m%d")
            hhmm = t.strftime("%H%M")
            ym = t.strftime("%Y.%m")
            for collector in collectors:
                key = (collector, ymd, hhmm)
                if key in seen:
                    continue
                seen.add(key)
                remote = f"https://data.ris.ripe.net/{collector}/{ym}/updates.{ymd}.{hhmm}.gz"
                local = f"{updates_root}/{collector}/{ym}/updates.{ymd}.{hhmm}.gz"
                tasks.append((remote, local))
    return tasks


tasks = build_tasks()

for case, cdate in CASES.items():
    ts = window_timestamps(cdate, WINDOW_DAYS, UPDATE_INTERVAL_MIN)
    print(
        f"{case:24s} crossing {cdate}  window {ts[0]:%Y-%m-%d} .. {ts[-1]:%Y-%m-%d}  ({len(ts)} slots)"
    )
print(
    f"{len(tasks):,} update files queued across {len(collectors)} collectors "
    f"(+/-{WINDOW_DAYS}d, {UPDATE_INTERVAL_MIN}-min)."
)


# %% [markdown]
# ## Verify what is on disk (resume state)


# %%
def disk_state(tasks):
    """Report how much of the queue is already downloaded (for pause/resume)."""
    present = present_bytes = missing = partial = 0
    for _, local in tasks:
        if os.path.exists(local) and os.path.getsize(local) > 0:
            present += 1
            present_bytes += os.path.getsize(local)
        else:
            missing += 1
        if os.path.exists(local + ".part"):
            partial += 1  # leftover from an interrupted download (harmless; re-fetched)
    return present, missing, partial, present_bytes


def print_disk_state(tasks):
    present, missing, partial, present_bytes = disk_state(tasks)
    print(
        f"on disk: {present:,}/{len(tasks):,} complete ({present_bytes / 1e9:.2f} GB) | "
        f"remaining: {missing:,} | leftover .part: {partial}"
    )
    return missing


# %% [markdown]
# ## Download (skips complete files; safe to Ctrl-C and re-run)


# %%
def download_one(task):
    remote, local = task
    if os.path.exists(local) and os.path.getsize(local) > 0:
        return ("skip", local)
    os.makedirs(os.path.dirname(local), exist_ok=True)
    tmp = local + ".part"
    try:
        urllib.request.urlretrieve(remote, tmp)
        os.replace(tmp, local)  # atomic: only complete files land at the final name
        return ("ok", local)
    except Exception as e:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        return ("fail", f"{remote} :: {e}")


if __name__ == "__main__":
    remaining = print_disk_state(tasks)

    if CHECK_ONLY:
        print("--check: disk state reported, not downloading.")
        sys.exit(0)

    results = {"ok": 0, "skip": 0, "fail": 0}
    failures = []
    with Pool(DOWNLOAD_WORKERS) as pool:
        for status, info in tqdm(
            pool.imap_unordered(download_one, tasks), total=len(tasks), desc="updates"
        ):
            results[status] += 1
            if status == "fail":
                failures.append(info)

    print(results)
    print(f"{len(failures)} failures (first 10):")
    for f in failures[:10]:
        print("  ", f)

    print_disk_state(tasks)
