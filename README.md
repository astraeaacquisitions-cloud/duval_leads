# Duval County Distress Docket

A lead-recon dashboard for Duval County, FL: pulls Official Records
(Lis Pendens, Judgments, Liens, Probate, Notices of Commencement,
Satisfactions/Releases) and the upcoming Tax Deed Auction, cross-references
owner/parcel data against the Property Appraiser, scores each record as a
motivated-seller lead, and publishes it as a filterable/sortable dashboard.

## Files

- `duval_leads_scraper.py` -- the data pipeline. Run:
  `python duval_leads_scraper.py --days 7 --out records.json`
  Requires `requests` and `beautifulsoup4` (`pip install requests beautifulsoup4`).
- `duval_dashboard.html` -- the static dashboard page. Reads `records.json`
  from the same directory at load time.

## Scheduled refresh

This repo is cloned by a scheduled cloud agent that re-runs the scraper and
republishes the dashboard's data on a recurring schedule.
