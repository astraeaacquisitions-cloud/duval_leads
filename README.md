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
- `duval_leads_db.py` -- the structured research/scoring layer (SQLite).
  See "Data ownership" below before touching this -- it is explicitly
  *not* the operational system of record yet.
- `import_history_to_sqlite.py` -- explicit, hand-run migration tool that
  imports `records.json` (or a `backup.json` export) into the SQLite
  layer. Always point `--db` at a throwaway path first to verify an
  import before running it against the real database.
- `config/property_type_rules.json` -- Phase 2 acquisition criteria: which
  Florida DOR Property Use codes are included, excluded, or flagged for
  review. Edit this file to change what the pipeline targets -- no code
  changes needed. This is the "manually enable" mechanism for apartments/
  commercial. `--include-all-property-types` on the scraper is a blunter,
  temporary override that disables the filter entirely.
- `config/dealability_weights.json` -- Phase 3 Dealability Score weights
  and thresholds. Edit to retune scoring -- no code changes needed.
- `config/tax_timeline.json` -- Phase 4 tax-deed timeline thresholds
  (imminent/soon day cutoffs) and closing-runway points per stage.

## Property-type classification (Phase 2)

Every record now carries `property_use_code`/`property_type_label` (the
Florida DOR use code straight from the Property Appraiser detail page --
e.g. `0100` / "Single Family"), `year_built`, and `building_count`.
Classification (`classify_property_type()` in `duval_leads_scraper.py`,
driven by `config/property_type_rules.json`) sorts every property into:

- **include** -- single family, vacant residential. These are never filtered out.
- **review** -- mobile homes, small multi-family, condos, co-ops, retirement
  homes, and any unrecognized code. Kept in the data, flagged
  `PROPERTY_TYPE_REVIEW_REQUIRED`, never silently included or excluded.
- **exclude** -- apartments (10+ units) and commercial/industrial/
  agricultural/institutional/government parcels. Filtered out of
  `records.json` by default, same as the existing ZIP/entity/unit-address
  exclusions -- reversible by editing the config file, or by rerunning
  with `--include-all-property-types`.

`TEARDOWN_INFILL_CANDIDATE` is a separate, informational flag (vacant
residential land, or a single structure built before 1960) -- a signal to
look at, not a score change and not a claim about actual condition or
redevelopment feasibility.

## Valuation and Dealability Score (Phase 3)

Every property now carries a `valuations` row (PAO assessed value, PAO
Just/Market value -- always shown and stored separately, never blended)
and a `scores` row with a Dealability Score (0-100). Two rules drive the
whole design:

1. **No fabricated equity dollar figure.** There is no free source for a
   mortgage balance, so "equity" is a labeled *signal*
   (`LIKELY_SUBSTANTIAL` / `LIKELY_MODERATE` / `LIKELY_LIMITED` /
   `UNKNOWN`) with a confidence (`ESTIMATED`/`INFERRED`/`UNKNOWN`) and a
   plain-language reason, derived from ownership tenure and appreciation
   -- never a dollar range that would just be the assessed value wearing
   a different name. `automated_estimated_value`, `estimated_asis_market_value`,
   `estimated_arv`, and `comparable_sales_used` stay `NULL` until a paid
   AVM/comp engine exists (a later phase) -- not backfilled from PAO data
   under a different name.
2. **Two-tier mortgage research.** A real payoff lookup (an Official
   Records name/parcel history search across *all* recorded mortgages,
   not just distress filings) is expensive per-property research, so it's
   never automatic. `mortgage_estimate` is always `'UNKNOWN'` until that
   lookup happens by hand. Properties that clear
   `tier2_mortgage_lookup_threshold` (default Dealability ≥ 50) get an
   `estimate_mortgage_payoff` research task queued instead -- "determine
   whether there's a plausible transaction before spending excessive time
   enriching the lead," per the original brief.

The dashboard always shows the required warning -- "County assessment is
a tax-related valuation and has not been verified as current market
value" -- next to the value fields, and the manageable-liens component of
Dealability counts real, already-scraped active distress documents on the
property (verified counts, not dollar amounts, which aren't captured).
"Closing runway" (20 of the 100 points) is a neutral placeholder until
Phase 4/5 build real tax-deed/foreclosure countdowns -- documented in the
score's own reasoning text, not hidden.

## Tax Deed Timeline (Phase 4)

Every property now also carries a `tax_deed_stage` and
`days_until_tax_sale`, computed in `compute_tax_deed_timeline()`
(`duval_leads_db.py`) and config-driven via `config/tax_timeline.json`.
This replaces the flat "closing runway" placeholder from Phase 3 for
tax-deed leads specifically -- every other category still uses the
placeholder pending Phase 5's foreclosure timeline.

**What this is built from, and what it isn't:** the *only* real, scraped
date in this pipeline is the scheduled tax deed auction date from
`duval.realtaxdeed.com` (stored as `documents.filed_date` for
`doc_type='TAX DEED'`). Florida's actual tax process has earlier stages --
delinquency (April 1), the tax certificate sale (~June), and the
certificate holder's 2-year eligibility window before they can even apply
for a deed -- and this pipeline does not scrape a source for any of
those. Rather than fabricate a 5-stage progression implying data we don't
have, a property with no scheduled auction on file is `TAX_STAGE_NONE` --
"not currently on the auction calendar," never "current on its taxes."

Stages, all derived purely from the countdown to that one verified date:

- `TAX_STAGE_FAR` -- auction scheduled, more than 90 days out. Ample time
  to close; full 20/20 closing-runway points.
- `TAX_STAGE_SOON` -- 31-90 days out. Workable but tightening.
- `TAX_STAGE_IMMINENT` -- 30 days or fewer. Real risk of running out of
  time to close before the county sells it -- this scores *lower*, not
  higher, than `FAR`: closing runway measures time available to close a
  deal, not seller motivation (a later phase's urgency/contact-priority
  score is where "imminent = more motivated" belongs).
- `TAX_STAGE_PAST_UNVERIFIED` -- the scheduled date has passed. The
  outcome (sold at auction, redeemed by the owner, postponed) isn't
  tracked by this pipeline, so this is flagged for manual verification,
  never assumed either way, and scores 0 closing-runway points until
  confirmed.
- `TAX_STAGE_NONE` -- no scheduled tax deed auction on file for this
  property (the overwhelming majority of non-tax-category leads). Keeps
  the same neutral 15/20 placeholder Phase 3 used for every lead.

**Known gap:** the real tax-delinquency and certificate-sale stages
(Florida's actual `TAX_STAGE_1`-`TAX_STAGE_3` equivalent, before a deed
application is ever filed) are not tracked because no source for them is
scraped -- the Duval County Tax Collector's delinquent-tax roll and
certificate-sale results live on a separate system from the tax deed
auction site this pipeline already reads. Flagged here as a real gap
rather than approximated from data that doesn't support it. Separately,
`duval.realtaxdeed.com` has returned HTTP 403 (bot-blocked) during recent
testing -- already handled gracefully by the existing scraper (skips tax
deed enrichment that run, logs it, and carries forward any previously
seen tax deed leads from history), not a Phase 4 regression.

## Data ownership

Several systems now hold overlapping pieces of the same picture. As of
this Phase 1 reconciliation, here is which one is authoritative for what:

| Data | Owned by | Notes |
|---|---|---|
| Operational lead/contact history (which distress filings exist, accumulated across runs) | `records.json`'s own accumulate-by-`doc_num` mechanism (in `duval_leads_scraper.py`) | The dashboard's only input. `--fresh` rebuilds it from scratch; otherwise every run merges into the existing file. **`records.json` is gitignored -- it only persists as long as the same container/disk does.** |
| Stable property/owner identity, evidence confidence, audit history of every run, export preparation | SQLite (`duval_leads_db.py`), durable via the git-tracked `data/backup.json` | Additive only. Reads the same accumulated `records` list the scraper already builds and layers structured fields on top (`property_id`, `owner_id`, `re_number`, confidence labels) -- it does not filter, reorder, or feed back into what `records.json` contains. |
| Qualified/contacted leads, conversations, follow-ups, deal pipeline | HubSpot (Phase 9, not yet built) | Only records explicitly approved via the dashboard's "Approve for HubSpot" control are ever exported -- never the raw scrape. |
| Portable backup, manual editing, recovery | CSV exports (`data/export_leads.csv`, `data/hubspot_export.csv`) and `data/backup.json` | Regenerable from SQLite at any time; `backup.json` is the one that's git-tracked and therefore the actual disaster-recovery copy. |
| Suppression flags, notes, HubSpot-approval marks made from the dashboard | The published artifact's `db` capability (`leads/{property_id}` docs) | A convenience write surface, not a vault -- intended to be synced into the durable database on each scheduled run, not treated as the only copy. |

**Known gap worth a decision:** because `records.json` is gitignored and
this pipeline runs in ephemeral cloud containers, the accumulate-by-`doc_num`
history only survives runs that happen to reuse the same container disk --
a fresh container starts that history over from nothing, even though SQLite's
`backup.json` (git-tracked) would still have it. Nothing in this phase
changes that; it's flagged here rather than fixed silently.

## Scheduled refresh

This repo is cloned by a scheduled cloud agent that re-runs the scraper and
republishes the dashboard's data on a recurring schedule.
