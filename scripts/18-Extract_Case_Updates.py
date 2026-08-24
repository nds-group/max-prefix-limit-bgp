# %% [markdown]
# # 11.7 Extract per-case BGP update events (Pool over files)
#
# Reads the local update MRT for each case study and saves the filtered A/W event records
# that 11.8 replays into a per-5-min state timeline (peer count / prefix count) for the plots.
#
# Heavy, order-independent parse -> parallelized over files (per coding prefs). The stateful
# replay is left to the 11.8 notebook (cheap, runs on these small filtered records).
#
# Per case:
#   Stage 1 (parallel): announcements originated by the case AS -> case prefix set + A records.
#   Stage 2 (parallel): withdrawals whose prefix is in that set  -> W records.
# Output: data/processed/updates_cases/{case}_events.pkl  (columns: ts, collector, peer_asn,
#         type[A/W], prefix, as_path), sorted by ts.
#
# Run in tmux:  activate env first, then
#               cd scripts
#               python 11.7-Extract_updates_use_cases.py            # all cases
#               python 11.7-Extract_updates_use_cases.py BCE        # substring-match one case

# %%
import os
import sys
import glob
import json
import datetime
from multiprocessing import Pool

import pandas as pd
from tqdm import tqdm
from pybgpkit_parser import Parser

from case_studies import (
    CASES, window_day_keys, window_anchor_timestamps, collector_of, is_case_prefix,
)

# %%
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(REPO_ROOT, "settings.json")) as fd:
    P = json.load(fd)

UPD = f"{P['DATA_RAW_DIR']}/updates/RIPE"
RIB = f"{P['DATA_RAW_DIR']}/RIBs/RIPE"
OUT = f"{P['DATA_DIR']}/processed/updates_cases"
NPROC = int(P["N_PROCESSES"])
os.makedirs(OUT, exist_ok=True)

# per-case globals, set before each Pool (inherited by forked workers on Linux)
_ASN = None
_IPV = None
_PREFIXES = frozenset()


def neighbor(as_path, asn):
    """AS adjacent to the case AS (hop before the origin, skipping prepends)."""
    a = as_path.split()
    s = str(asn)
    i = len(a) - 1
    while i >= 0 and a[i] == s:
        i -= 1
    return a[i] if i >= 0 else None


# %%
def seed_worker(f):
    """Standing RIB paths to case prefixes in one window-start bview (for cold-start).

    Store the full as_path so the 11.8 replay can apply BOTH the paper's metrics:
      - announced-prefix visibility (nb 4/6): a prefix counts at a collector only if
        its path traverses a Tier-1 AS (case AS is the origin, hence downstream);
      - peer count (nb 3/7): the AS adjacent to the case AS (neighbor).
    """
    coll = collector_of(f)
    rec = []
    try:
        for e in Parser(url=f, filters={"origin_asn": str(_ASN)}):
            if e.as_path and is_case_prefix(e.prefix, _IPV):
                rec.append((coll, e.peer_asn, e.prefix, e.as_path))
    except Exception:
        return []
    return rec


def announce_worker(f):
    """Announcements whose path contains _ASN (case family) in one file.

    as_path regex (not origin_asn) so we also capture paths where the case AS is TRANSIT
    for its downstream customers -- those updates carry the real timing of upstream (e.g.
    Tier-1) sessions dropping, which origin-only extraction misses. The replay separates
    the two: peers uses ALL adjacencies, announced-prefixes uses origin paths only.
    """
    coll = collector_of(f)
    rec = []
    try:
        for e in Parser(url=f, filters={"as_path": rf"\b{_ASN}\b"}):
            if e.elem_type == "A" and is_case_prefix(e.prefix, _IPV):
                rec.append((e.timestamp, coll, e.peer_asn, "A", e.prefix, e.as_path))
    except Exception:
        return []
    return rec


def withdraw_worker(f):
    """Withdrawals whose prefix is in _PREFIXES in one file."""
    coll = collector_of(f)
    rec = []
    try:
        for e in Parser(url=f, filters={"type": "withdraw"}):
            if e.prefix in _PREFIXES:
                rec.append((e.timestamp, coll, e.peer_asn, "W", e.prefix, None))
    except Exception:
        return []
    return rec


def case_files(case):
    """All collectors' update files over the case window."""
    files = []
    for ym, ymd in window_day_keys(case["crossing"], case["window_days"]):
        files += glob.glob(f"{UPD}/*/{ym}/updates.{ymd}.*.gz")
    return sorted(files)


def seed_case(name, case):
    """Standing RIB state at window start (all collectors) -> {case}_seed.pkl, to seed
    the 11.8 replay so peer/prefix counts start at their true value (cold-start fix)."""
    global _ASN, _IPV
    _ASN, _IPV = case["asn"], case["ipv"]
    ym, ymd = window_day_keys(case["crossing"], case["window_days"])[0]  # window-start day
    files = sorted(glob.glob(f"{P['DATA_RAW_DIR']}/RIBs/RIPE/*/{ym}/bview.{ymd}.0000.gz"))
    print(f"[{name}] seed from {len(files)} bview RIBs @ {ymd}.0000 (big files, be patient)")
    seed = []
    with Pool(NPROC) as pool:
        for rec in tqdm(pool.imap_unordered(seed_worker, files, chunksize=1),
                        total=len(files), desc=f"{name} seed"):
            seed.extend(rec)
    df = pd.DataFrame(seed, columns=["collector", "peer_asn", "prefix", "as_path"])
    out = f"{OUT}/{name}_seed.pkl"
    df.to_pickle(out)
    print(f"[{name}] seed {len(df):,} RIB paths @ window start -> {out}")
    return out


def bview_ts(path):
    """Unix timestamp from a .../bview.YYYYMMDD.HHMM.gz path."""
    tag = os.path.basename(path).split("bview.")[1].rsplit(".gz", 1)[0]  # YYYYMMDD.HHMM
    dt = datetime.datetime.strptime(tag, "%Y%m%d.%H%M").replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp())


def anchor_worker(f):
    """All paths (any position of the case AS) to case-family prefixes in one anchor bview.

    Uses an as_path regex so we capture adjacencies where the case AS is TRANSIT (its
    downstream customers), not only where it originates -- matching the notebook-3 peer
    graph. The 11.8 replay resets state to these records at each 8h anchor (re-anchoring).
    """
    ats = bview_ts(f)
    coll = collector_of(f)
    rec = []
    try:
        for e in Parser(url=f, filters={"as_path": rf"\b{_ASN}\b"}):
            if e.as_path and is_case_prefix(e.prefix, _IPV):
                rec.append((ats, coll, e.peer_asn, e.prefix, e.as_path))
    except Exception:
        return []
    return rec


def anchor_files(case):
    """All collectors' bview files at every 8h anchor over the case window."""
    files = []
    for t in window_anchor_timestamps(case["crossing"], case["window_days"]):
        ym, ts = t.strftime("%Y.%m"), t.strftime("%Y%m%d.%H%M")
        files += glob.glob(f"{RIB}/*/{ym}/bview.{ts}.gz")
    return sorted(files)


def anchor_case(name, case):
    """Parse the RIB at every 8h anchor (all collectors) -> {case}_anchors.pkl, the ground
    truth the 11.8 replay resets to each 8h (kills update-stream drift / zombies)."""
    global _ASN, _IPV
    out = f"{OUT}/{name}_anchors.pkl"
    if os.path.exists(out):
        print(f"[{name}] anchors exist, skipping -> {out}")
        return out
    _ASN, _IPV = case["asn"], case["ipv"]
    files = anchor_files(case)
    n_anchors = len(window_anchor_timestamps(case["crossing"], case["window_days"]))
    print(f"[{name}] anchors: {len(files)} bviews over {n_anchors} 8h marks (big files, be patient)")
    recs = []
    with Pool(NPROC) as pool:
        for r in tqdm(pool.imap_unordered(anchor_worker, files, chunksize=1),
                      total=len(files), desc=f"{name} anchors"):
            recs.extend(r)
    df = pd.DataFrame(recs, columns=["anchor_ts", "collector", "peer_asn", "prefix", "as_path"])
    df.to_pickle(out)
    print(f"[{name}] {len(df):,} anchor records over {df.anchor_ts.nunique()} anchors -> {out}")
    return out


def extract_case(name, case):
    global _ASN, _IPV, _PREFIXES
    out = f"{OUT}/{name}_events.pkl"
    if os.path.exists(out):
        print(f"[{name}] events exist, skipping extract -> {out}")
        return out
    _ASN, _IPV = case["asn"], case["ipv"]
    files = case_files(case)
    print(f"[{name}] AS{_ASN} IPv{_IPV} | {len(files)} files")

    # stage 1: case-originated announcements -> prefix set
    ann = []
    with Pool(NPROC) as pool:
        for rec in tqdm(pool.imap_unordered(announce_worker, files, chunksize=8),
                        total=len(files), desc=f"{name} A"):
            ann.extend(rec)
    _PREFIXES = frozenset(r[4] for r in ann)
    print(f"[{name}] {len(ann):,} A records | {len(_PREFIXES)} case prefixes")

    # stage 2: withdrawals of those prefixes
    wdr = []
    with Pool(NPROC) as pool:
        for rec in tqdm(pool.imap_unordered(withdraw_worker, files, chunksize=8),
                        total=len(files), desc=f"{name} W"):
            wdr.extend(rec)
    print(f"[{name}] {len(wdr):,} W records")

    df = pd.DataFrame(ann + wdr, columns=["ts", "collector", "peer_asn", "type", "prefix", "as_path"])
    df = df.sort_values("ts", kind="stable").reset_index(drop=True)
    df.to_pickle(out)
    print(f"[{name}] saved {len(df):,} events -> {out}\n")
    return out


# %%
if __name__ == "__main__":
    # usage: python 11.7-...py [case-substring] [stage]   stage in {anchors, events, both(default)}
    only = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "all" else None
    stage = sys.argv[2] if len(sys.argv) > 2 else "both"
    for name, case in CASES.items():
        if only and only.lower() not in name.lower():
            continue
        if stage in ("anchors", "both"):
            anchor_case(name, case)   # 8h RIB ground truth for re-anchoring the replay
        if stage in ("events", "both"):
            extract_case(name, case)  # 5-min update events applied between anchors
