# CAIDA AS Rank (raw, NOT in git except the downloader)

The pipeline uses `asns.jsonl` (one AS per line: `asn`, `rank`, `organization`, ...)
for the AS-rank classification (Major / Regional / Peripheral). The Tier-1 set itself
comes from `settings.json` `TIER1` (authoritative); CAIDA rank fills in the rest.

## How to download

CAIDA's own client is shipped here: `asrank-download.py`. It needs the `graphqlclient`
package (not in `requirements.txt` by default, since it is only used for this step):

    pip install graphqlclient
    python data/raw/AS_rank/asrank-download.py -a data/raw/AS_rank/asns.jsonl

Flags: `-a` asns, `-o` organizations, `-l` asn links, `-d N` smaller debug batch.
The full `asns.jsonl` is ~59 MB (~90k+ ASes). Source: https://asrank.caida.org/

Note: CAIDA's `cliqueMember` field is unreliable for Tier-1 detection (true for tens of
thousands of ASes); do not use it. Tier-1 = `settings.json` `TIER1`.
