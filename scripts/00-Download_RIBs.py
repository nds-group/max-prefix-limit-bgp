#!/usr/bin/env python3
"""Download RIPE RIS RIB snapshots (8-hourly) for the study period.

Reads `settings.json` at the repo root and writes to
`DATA_RAW_DIR/RIBs/RIPE/{collector}/{YYYY.MM}/bview.{YYYYMMDD}.{HHMM}.gz`.

A full run is large (~3.4 TB across all collectors and the year). Use `--limit`
(and optionally `--collectors`) to fetch just a few files, e.g. to validate the
setup:

    python scripts/0-Download_RIBs.py --limit 1 --collectors rrc00

Files are downloaded atomically (`.part` -> rename) and existing files are skipped,
so the command is safe to interrupt and re-run.
"""
import os
import json
import argparse
import datetime
import urllib.request

from tqdm import tqdm

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def resolve(path):
    """Resolve a settings.json path relative to the repo root (absolute passes through)."""
    return path if os.path.isabs(path) else os.path.normpath(os.path.join(REPO_ROOT, path))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=0,
                    help="max number of files to download (0 = all); small value validates the setup")
    ap.add_argument("--collectors", nargs="*", default=None,
                    help="override the collector list from settings.json")
    args = ap.parse_args()

    with open(os.path.join(REPO_ROOT, "settings.json")) as fd:
        params = json.load(fd)

    data_raw_dir = resolve(params["DATA_RAW_DIR"])
    collectors = args.collectors or params["COLLECTORS"]
    start = datetime.datetime.strptime(params["START_DATE"], "%Y-%m-%d")
    end = datetime.datetime.strptime(params["END_DATE"], "%Y-%m-%d")

    times, t = [], start
    while t < end:
        times.append(t)
        t += datetime.timedelta(hours=8)

    downloaded = 0
    for collector in collectors:
        for time_value in tqdm(times, desc=f"RIB {collector}"):
            if args.limit and downloaded >= args.limit:
                print(f"Reached --limit {args.limit}; stopping.")
                return
            time_str = time_value.strftime("%Y%m%d.%H%M")
            year_month = time_value.strftime("%Y.%m")

            url = f"https://data.ris.ripe.net/{collector}/{year_month}/bview.{time_str}.gz"
            out_dir = os.path.join(data_raw_dir, "RIBs", "RIPE", collector, year_month)
            out_file = os.path.join(out_dir, f"bview.{time_str}.gz")
            os.makedirs(out_dir, exist_ok=True)

            if os.path.exists(out_file):
                continue
            try:
                tmp = out_file + ".part"
                urllib.request.urlretrieve(url, tmp)
                os.replace(tmp, out_file)
                downloaded += 1
            except Exception as e:
                print(f"Failed to download {url}: {e}")


if __name__ == "__main__":
    main()
