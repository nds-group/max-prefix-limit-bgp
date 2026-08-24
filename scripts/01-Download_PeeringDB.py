#!/usr/bin/env python3
"""Download daily PeeringDB snapshots (CAIDA mirror) for the study period.

Reads `settings.json` at the repo root and writes JSON dumps to
`DATA_RAW_DIR/peeringdb/peeringdb_2_dump_{YYYY}_{MM}_{DD}.json`.

Default schedule: Jan 1 / Jul 1 anchors of 2022-2025 plus every day of 2025
(the freshness set used by notebook 2). Use `--dates` to fetch specific days or
`--limit` to cap, e.g. to validate the setup:

    python scripts/1-Download_PeeringDB.py --dates 2025-01-02 --sleep 0

Source: https://publicdata.caida.org/datasets/peeringdb/
"""
import os
import json
import time
import argparse
import datetime
import urllib.request
import urllib.error

from tqdm import tqdm

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def resolve(path):
    return path if os.path.isabs(path) else os.path.normpath(os.path.join(REPO_ROOT, path))


def default_dates():
    dates = [datetime.datetime(y, m, 1) for y in (2022, 2023, 2024, 2025) for m in (1, 7)]
    d = datetime.datetime(2024, 12, 31)
    while d < datetime.datetime(2026, 1, 1):
        dates.append(d)
        d += datetime.timedelta(days=1)
    return sorted(set(dates))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dates", nargs="*", default=None,
                    help="explicit dates YYYY-MM-DD to fetch (default: the built-in schedule)")
    ap.add_argument("--limit", type=int, default=0, help="max files to download (0 = all)")
    ap.add_argument("--sleep", type=float, default=10.0,
                    help="seconds to wait between downloads (be polite; 0 for a quick test)")
    args = ap.parse_args()

    with open(os.path.join(REPO_ROOT, "settings.json")) as fd:
        params = json.load(fd)
    out_dir = os.path.join(resolve(params["DATA_RAW_DIR"]), "peeringdb")
    os.makedirs(out_dir, exist_ok=True)

    if args.dates:
        dates = [datetime.datetime.strptime(d, "%Y-%m-%d") for d in args.dates]
    else:
        dates = default_dates()

    downloaded = 0
    for date in tqdm(dates, desc="Downloading peeringdb"):
        if args.limit and downloaded >= args.limit:
            print(f"Reached --limit {args.limit}; stopping.")
            return
        y, m, d = date.year, date.month, date.day
        name = f"peeringdb_2_dump_{y}_{m:02d}_{d:02d}.json"
        url = f"https://publicdata.caida.org/datasets/peeringdb/{y}/{m:02d}/{name}"
        local = os.path.join(out_dir, name)
        if os.path.exists(local):
            continue
        try:
            tmp = local + ".part"
            urllib.request.urlretrieve(url, tmp)
            os.replace(tmp, local)
            downloaded += 1
            if args.sleep:
                time.sleep(args.sleep)
        except urllib.error.HTTPError as e:
            print(f"{'Not found' if e.code == 404 else 'Error'} for {url}: {e}")
        except Exception as e:
            print(f"Failed to download {url}: {e}")


if __name__ == "__main__":
    main()
