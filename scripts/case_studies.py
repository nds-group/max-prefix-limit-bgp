"""Single source of truth for the four case studies (updates pipeline: 11.6 / 11.7 / 11.8).

Import this from the fetch, extract, and plot steps so the four cases are defined once.
"""
import datetime

# asn        : origin AS of the case study
# ipv        : 4 or 6 (which family the crossing is in)
# crossing   : the +/- window is centered on this date (paper Section 6.3)
# window_days: +/- days around the crossing
# highlight  : peers worth highlighting in the plots (ASN -> label); the replay still
#              recovers the full peer set, this is only for annotating key losses
CASES = {
    "AS25273_BCE_v4": {
        "asn": 25273, "ipv": 4,
        "crossing": datetime.date(2025, 9, 5), "window_days": 1,
        "highlight": {3356: "Level3", 1299: "Arelion", 174: "Cogent", 208374: "LU-CIX"},
    },
    "AS52920_IVOCS_v4": {
        "asn": 52920, "ipv": 4,
        "crossing": datetime.date(2025, 8, 12), "window_days": 1,
        "highlight": {},
    },
    "AS44901_BelCloud_v6": {
        "asn": 44901, "ipv": 6,
        "crossing": datetime.date(2025, 1, 15), "window_days": 1,
        "highlight": {28917: "AS28917", 50263: "AS50263", 29076: "AS29076"},
    },
    "AS52603_SupplyNet_v6": {
        "asn": 52603, "ipv": 6,
        "crossing": datetime.date(2025, 9, 2), "window_days": 1,
        "highlight": {6939: "HE", 24482: "AS24482"},
    },
}


def window_timestamps(crossing, window_days, interval_min=5):
    """All 5-min timestamps over [crossing - window_days, crossing + window_days] inclusive."""
    start = datetime.datetime.combine(
        crossing - datetime.timedelta(days=window_days), datetime.time.min
    )
    end = datetime.datetime.combine(
        crossing + datetime.timedelta(days=window_days), datetime.time.max
    )
    step = datetime.timedelta(minutes=interval_min)
    out, t = [], start
    while t <= end:
        out.append(t)
        t += step
    return out


def window_anchor_timestamps(crossing, window_days, hours=8):
    """8h RIB anchor datetimes (00:00/08:00/16:00 ...) over the window, for re-anchoring."""
    start = datetime.datetime.combine(
        crossing - datetime.timedelta(days=window_days), datetime.time.min
    )
    end = datetime.datetime.combine(
        crossing + datetime.timedelta(days=window_days), datetime.time.max
    )
    step = datetime.timedelta(hours=hours)
    out, t = [], start
    while t <= end:
        out.append(t)
        t += step
    return out


def window_day_keys(crossing, window_days):
    """(year_month, yyyymmdd) pairs covering the window, for globbing update files."""
    keys = set()
    for t in window_timestamps(crossing, window_days):
        keys.add((t.strftime("%Y.%m"), t.strftime("%Y%m%d")))
    return sorted(keys)


def collector_of(path):
    """rrcNN from a .../RIPE/<collector>/<yyyy.mm>/updates.*.gz path."""
    return path.split("/RIPE/")[1].split("/")[0]


def is_case_prefix(prefix, ipv):
    """True if `prefix` is in the case's address family."""
    return (":" in prefix) == (ipv == 6)
