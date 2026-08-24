# RIPE RIS RIB snapshots (raw, NOT in git)

8-hourly RIB dumps (00:00/08:00/16:00 UTC) from 23 RIS collectors, 2025.
~750 GB. Download with `scripts/00-Download_RIBs.py` (reads `settings.json`).
Files land as `{collector}/{YYYY.MM}/bview.{YYYYMMDD}.{HHMM}.gz`.
Source: https://ris.ripe.net/
