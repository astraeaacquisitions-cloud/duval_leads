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
- `config/foreclosure_timeline.json` -- Phase 5 foreclosure timeline
  thresholds (the post-judgment "stale" cutoff) and closing-runway points
  per stage.
- `config/code_enforcement.json` -- Phase 6 code enforcement signal
  thresholds (the "recent vs. aged" single-violation cutoff).
- `config/seller_profile.json` -- Phase 7 seller-profile thresholds (the
  multi-property portfolio cutoff).
- `config/stacked_distress_weights.json` -- Phase 8 distress/urgency/
  confidence weights, the contact-priority blend, the low-confidence
  cap, and the A-E tier cutoffs.
- `sync_hubspot_approvals.py` -- Phase 9, explicit hand-run sync tool
  that applies a JSON dump of the dashboard's `leads` collection (fetch
  with the ArtifactData tool) into SQLite, so `export_hubspot_csv()`
  reflects what was actually approved. Same "throwaway `--db` first"
  discipline as `import_history_to_sqlite.py`.
- `config/outreach_angles.json` -- Phase 9 rule-based outreach-angle
  templates. Edit to change the suggested talking points -- no code
  changes needed.

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
"Closing runway" (20 of the 100 points) uses real tax-deed (Phase 4) or
foreclosure (Phase 5) timeline data when either exists for a property,
and falls back to a neutral 15-point placeholder otherwise -- documented
in the score's own reasoning text, not hidden.

## Tax Deed Timeline (Phase 4)

Every property now also carries a `tax_deed_stage` and
`days_until_tax_sale`, computed in `compute_tax_deed_timeline()`
(`duval_leads_db.py`) and config-driven via `config/tax_timeline.json`.
This replaces the flat "closing runway" placeholder from Phase 3 for
tax-deed leads specifically; foreclosure leads use Phase 5's timeline
instead (below), and every other category still uses the placeholder.

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

## Foreclosure Timeline (Phase 5)

Every property now also carries a `foreclosure_stage` and
`days_since_foreclosure_milestone`, computed in
`compute_foreclosure_timeline()` (`duval_leads_db.py`) and config-driven
via `config/foreclosure_timeline.json`. This feeds the same "closing
runway" component of the Dealability Score as Phase 4, for leads that
don't already have a tax-deed timeline (tax-deed wins when a property
somehow has both, since it's the more precise -- exact date -- signal).

**What this is built from, and what it isn't:** unlike the tax deed
auction site, Duval's Official Records site never gives an exact sale
date -- there is no scraped foreclosure-sale-calendar source at all. So
this is built from two real, verified judicial milestones already
captured in `documents`: the Lis Pendens filing date (the case's opening
filing, `cat='foreclosure'`), and, once one exists, the Final Judgment
date (`doc_type` `RPO FINAL JUDGMENT` or `VA FINAL JUDGMENT`,
`cat='judgment'` -- deliberately *not* any generic `JUDGMENT`, which is
far more common and isn't evidence a sale is anywhere close). No exact
days-until-sale is ever claimed -- only real elapsed time since a real
recorded milestone, plus Florida Statute 45.031's typical 20-35-day
post-judgment sale window cited as legal context, not a scraped fact.

Stages:

- `FORECLOSURE_STAGE_FILED` -- a Lis Pendens is on file, no final
  judgment yet. Case duration varies widely (months to years depending on
  contested litigation, bankruptcy stays, mediation, etc.), so this is
  genuinely uncertain -- it scores a moderate 12/20 closing-runway points,
  not high or low, and the day count shown is informational only, never a
  countdown.
- `FORECLOSURE_STAGE_JUDGMENT_ENTERED` -- a final judgment was entered
  within the last `judgment_recent_days` (default 45). Per FL Statute
  45.031 the sale is typically scheduled 20-35 days after judgment, so
  one is likely imminent even without an exact scraped date -- scores a
  low 4/20 points, same logic as Phase 4's `TAX_STAGE_IMMINENT` (closing
  runway measures time available to close, not seller motivation).
- `FORECLOSURE_STAGE_JUDGMENT_STALE` -- a final judgment exists but is
  older than the statutory window. The outcome (sold, postponed,
  redeemed, case dismissed) isn't tracked by this pipeline, so this is
  flagged for manual verification rather than assumed either way, and
  scores 0 points until confirmed.
- `FORECLOSURE_STAGE_NONE` -- no Lis Pendens on file for this property
  (every non-foreclosure lead, and any foreclosure-category lead whose
  Property Appraiser match failed to resolve a `property_id`). Keeps the
  same neutral 15/20 placeholder as `TAX_STAGE_NONE`.

**Known gap:** this pipeline has no source for the actual foreclosure
sale date, sale results, or whether a sale was postponed/cancelled --
that would require scraping the Duval County Clerk's foreclosure sale
calendar (a separate system from the Official Records search this
pipeline already reads), similar in spirit to what Phase 4 did for tax
deeds. `FORECLOSURE_STAGE_JUDGMENT_STALE` exists specifically to flag
that gap rather than silently guessing an outcome once the statutory
window has plausibly passed.

## Code Enforcement Signal (Phase 6)

Every property now also carries a `code_enforcement_stage` and
`code_violation_count`, computed in `compute_code_enforcement_signal()`
(`duval_leads_db.py`) and config-driven via
`config/code_enforcement.json`. Each individual code-violation document
also carries a `code_violation_confidence` (`STRONG`/`MODERATE`).

**What this is built from, and what it isn't:** Duval's Official Records
has no distinct "Code Violation" document type at all -- this has always
been an *inferred lien-proxy* (a generic `LIEN` record filed by a
city/county entity rather than a bank, HOA, or contractor; see
`MUNICIPAL_FILER_PATTERN` in the scraper, unchanged from before this
phase). Phase 6 adds two real, defensible refinements on top of that
existing inference, without touching or narrowing the underlying
detection:

1. **Confidence tiering.** Not every municipal filer is a code
   violation specifically -- "CITY OF"/"DUVAL COUNTY" could just as
   easily be a demolition lien, a nuisance-abatement lien, or an unpaid
   utility lien. Only a filer name that literally says "CODE
   ENFORCEMENT"/"CODE COMPLIANCE"/"MUNICIPAL CODE" is unambiguous
   (`STRONG`); the broader municipal-filer match is real signal but
   weaker (`MODERATE`). Both still count as a code-violation lien for
   filtering/scoring purposes -- this only labels how confident that
   specific classification is, never removes a record from the
   category.
2. **A chronic-pattern signal**, `CODE_STAGE_REPEAT` -- a property with
   *two or more* code-violation liens recorded against it, which is real
   and countable (not an inference stacked on an inference): a genuine
   chronic-neglect pattern. Stages:
   - `CODE_STAGE_NONE` -- no code-violation lien on file (most leads).
   - `CODE_STAGE_SINGLE_RECENT` -- exactly one, filed within the last
     `recent_days` (default 180).
   - `CODE_STAGE_SINGLE_AGED` -- exactly one, older than that. This
     pipeline can't confirm whether it was ever resolved -- there's no
     reliable cross-reference from a `SATISFACTION`/`RELEASE` record
     back to the specific lien it clears, so "aged" is informational
     recency, never a claim the case is closed.
   - `CODE_STAGE_REPEAT` -- two or more. Also sets the
     `CHRONIC_CODE_VIOLATIONS` flag (shown as a review badge in the
     dashboard modal).

**Deliberately not wired into the Dealability Score.** Chronic code
violations are a seller-*distress*/motivation signal, not a deal-
*feasibility* signal -- Dealability's existing "manageable liens"
component already counts every code-violation document toward
`active_lien_count` like any other active distress record, so the
scoring impact already exists there. This phase's job is to surface and
label the richer signal (confidence + chronicity) for the
stacked-distress/contact-priority engine that a later phase builds, not
to retrofit the dealability formula.

**Known gap:** no fine amount, case status, or hearing date is scraped
for any of this -- Duval's actual Municipal Code Compliance case
tracking lives on a separate system from the Official Records lien
search this pipeline reads. Every stage and label here is built strictly
from the fact that a municipal lien was recorded, and how many times,
never from a fabricated case status.

## Seller Profile Signals (Phase 7)

Every property now also carries an `absentee_stage`, a
`landlord_portfolio_stage`, and (when portfolio data applies)
`portfolio_property_count`/`portfolio_distressed_count`, computed in
`compute_absentee_signal()` and `compute_landlord_portfolio_signal()`
(`duval_leads_db.py`), config-driven via `config/seller_profile.json`.
Probate leads also now generate a research task and a
`PROBATE_REPRESENTATIVE_REVIEW_REQUIRED` flag.

**Absentee ownership** compares each current owner's mailing address
against the property's own situs address (normalized the same way
identity hashing already is, so formatting differences don't produce a
false flag):

- `ABSENTEE_STAGE_OWNER_OCCUPIED_LIKELY` -- mailing address matches the
  property address.
- `ABSENTEE_STAGE_LOCAL_ABSENTEE` -- mailing address differs but is
  still in Florida.
- `ABSENTEE_STAGE_OUT_OF_STATE` -- mailing address is outside Florida,
  sets the `OUT_OF_STATE_OWNER` flag. The strongest absentee signal: a
  distant property is genuinely harder to manage or maintain.
- `ABSENTEE_STAGE_UNKNOWN` -- insufficient address data to compare.
  Missing data is never silently treated as owner-occupied.

**Landlord portfolio / fatigue** counts DISTINCT properties linked to
the same owner(s) in this pipeline's own data:

- `LANDLORD_STAGE_SINGLE` -- only one property on file for this owner.
- `LANDLORD_STAGE_MULTI_PROPERTY` -- 2+ properties on file, but only
  this one currently carries an active distress document.
- `LANDLORD_STAGE_MULTI_PROPERTY_DISTRESS` -- 2+ of this owner's
  properties currently carry an active distress document at once (sets
  `LANDLORD_MULTI_PROPERTY_DISTRESS`) -- a portfolio owner facing
  trouble on multiple holdings simultaneously, the real signal behind
  "landlord fatigue," not an isolated case.
- `LANDLORD_STAGE_UNKNOWN` -- no confirmed owner on file.

**Portfolio counts are a floor, not a ceiling.** This pipeline's owner
identity matching is deliberately conservative (see "Identity model" in
`duval_leads_db.py`'s module docstring) -- under-merging is a research
task later; over-merging corrupts identity data silently, which is much
worse. So the same real landlord could show up as several different
`owner_id`s across inconsistently formatted filings, and this signal
would under-count their true portfolio. Documented here rather than
overstated in the UI.

**Probate:** the party named on a probate record (`DirectName`) is
conventionally the decedent -- a legitimate estate-sale lead, but never
a contactable person. Same discipline as trust ownership (Phase 1): a
`verify_probate_representative` research task and
`PROBATE_REPRESENTATIVE_REVIEW_REQUIRED` flag are generated so nobody
assumes a relative, heir, or occupant has authority to sell without
confirming the actual personal representative.

**Deliberately not wired into the Dealability Score**, same reasoning
as Phase 6: these are seller-distress/motivation signals for the
stacked-distress/contact-priority engine a later phase builds, not
deal-feasibility ones.

**Explicitly not built -- eviction and vacancy.** Duval's Official
Records has no eviction/landlord-tenant case data at all; that lives in
a separate court case management system this pipeline does not scrape.
There is also no free, reliable source to verify whether a property is
actually vacant. Rather than approximate either from data that doesn't
support it (e.g. treating "absentee + old building" as a vacancy proxy),
both are left out entirely and documented as a real gap.

## Stacked-Distress Scoring, Urgency, Confidence & Contact Priority (Phase 8)

This is where every signal built in Phases 2-7 finally gets consumed.
The `scores` table has carried four nullable columns
(`distress_score`, `urgency_score`, `confidence_score`,
`contact_priority_score`, plus `tier`/`tier_reason`) since Phase 3's
`dealability_score` was the only one populated -- Phase 8 fills in the
rest, in `compute_distress_score()`, `compute_urgency_score()`,
`compute_confidence_score()`, and `compute_contact_priority_and_tier()`
(`duval_leads_db.py`), config-driven via
`config/stacked_distress_weights.json`. No new data source -- every
input was already computed and persisted by an earlier phase.

**Four scores answer four different questions**, deliberately kept
separate rather than folded into one number:

- **Dealability** (Phase 3) -- *is this a workable deal?* Equity signal,
  manageable liens, ownership clarity, acquisition fit, value
  reliability, closing runway.
- **Distress** -- *how much real legal/financial trouble is this
  property in?* Rewards breadth (distinct active distress categories on
  one property -- the Compound Distress view's Tier 1/2/3 concept,
  recomputed server-side here) and depth (document count, plus an
  advanced-stage bonus when the tax-deed, foreclosure, or code
  enforcement timeline shows the distress has actually progressed, not
  just that a lien was filed).
- **Urgency** -- *how soon might something irreversible happen?* The
  deliberate mirror image of Dealability's closing-runway component: an
  imminent tax deed auction or a recently entered foreclosure judgment
  means LOW time to close (bad for Dealability) but HIGH urgency (a
  highly motivated seller, or a deal that disappears if not acted on
  now). Reuses the exact same tax-deed/foreclosure timeline data Phases
  4-5 already computed, with tax-deed taking precedence when a property
  somehow has both (the more precise, exact-date signal).
- **Confidence** -- *how well do we actually know this lead, and can we
  identify who to contact?* Data quality, not deal quality. Address
  match confidence, owner identity confidence, whether a PAO value
  matched, minus a penalty per review-required flag on file
  (`TRUST_OWNERSHIP_REVIEW_REQUIRED`, `PROBATE_REPRESENTATIVE_REVIEW_
  REQUIRED`, `PROPERTY_TYPE_REVIEW_REQUIRED`) -- a high Dealability
  Score on a lead we can't verify or don't know who to contact isn't a
  real opportunity yet.

**Contact Priority** blends all four (Dealability 35%, Urgency 30%,
Distress 20%, Confidence 15%) into the master ranking, then applies a
hard cap: when Confidence is below a threshold (default 25), priority
is capped at 50 regardless of how good the rest looks -- a lead this
uncertain can't be a top priority no matter how distressed, urgent, or
dealable it appears, though it's still worth researching (capped, not
zeroed). **Lead Tier** (A-E) buckets the final Contact Priority Score
against config-driven cutoffs (A ≥ 80 down to E < 30), each with a
plain-language `tier_reason` showing the four underlying scores.

**Distinct from the Compound Distress view's own "Tier 1/2/3."** That
number (how many distress *documents* stack on one property, computed
client-side in the dashboard) predates Phase 8 and still drives the
Compound Distress table's own Tier column; the new A-E letter is the
Contact Priority tier and gets its own "Priority" column and filter
(`tier-filter`) so the two are never confused on screen.

The dashboard shows a new "Stacked Distress & Priority" modal section
(all four scores plus the tier badge and every reasoning string) in
both the single-filing and Compound Distress modals, and a compact
"Priority" column (the A-E badge) sortable by `contact_priority_score`
in both table views.

## HubSpot Export & Outreach (Phase 9)

**What this phase actually is: an import-ready CSV, not a live push.**
This environment has no HubSpot API key or MCP connector configured, so
there is no live HubSpot integration to build against -- claiming one
would violate the same "never claim data or capability we don't have"
discipline every earlier phase followed. What Phase 9 delivers instead:
a CSV shaped for HubSpot's own contact-import wizard, gated by the same
explicit-approval rule the pipeline has had since Phase 1, now actually
wired end to end and enriched with the Phase 3-8 scoring context.

**The gap this phase closes.** The dashboard's "Approve for HubSpot
export" checkbox has always written straight to the published artifact's
own `db` capability (a `leads/{property_id}` document per property) --
that's documented in "Data ownership" below as a convenience write
surface, "intended to be synced into the durable database on each
scheduled run." Nothing ever did that sync: `merge_dashboard_state()`
existed in `duval_leads_db.py` since Phase 1 but nothing called it, so
`hubspot_export_flags` -- and therefore `export_hubspot_csv()` -- stayed
permanently empty no matter what got approved in the dashboard. Phase 9
adds the other half: `duval_leads_db.sync_dashboard_leads_state()`
adapts the dashboard's `leads` collection into `merge_dashboard_state()`'s
shape, and `sync_hubspot_approvals.py` is the explicit, hand-run script
that applies it -- same pattern and same "test against a throwaway `--db`
first" discipline as `import_history_to_sqlite.py`. This has to be a
deliberate step, not an automatic one: the standalone scraper process is
plain Python with no browser/JS context, so it cannot read the artifact's
`db` capability itself -- only a session with the `ArtifactData` tool
(fetching the published dashboard's `leads` collection) can produce the
JSON file this script consumes.

**What the export now includes.** `export_hubspot_csv()` still only
ever includes rows with an explicit `approved_by` -- nothing changed
about that gate -- but each row now also carries the full Phase 3-8
picture: Dealability/Distress/Urgency/Confidence/Contact Priority
scores, Tier, the plain-language Tier Reason, every review/portfolio/
absentee flag on file, and the outreach angle described below. Map
these to HubSpot custom properties in the import wizard so a rep isn't
starting from a bare name and address.

**Suggested Outreach Angle** (`compute_outreach_angle()`) is a rule-
based conversation starter assembled ONLY from signals this pipeline
already computed -- never free-form generated text, and never a claim
about the owner's personal situation beyond what the recorded documents
show. It combines every applicable fragment (an out-of-state landlord
with chronic code violations gets both angles, not just one), always
leading with the review-required caveat when
`TRUST_OWNERSHIP_REVIEW_REQUIRED` or
`PROBATE_REPRESENTATIVE_REVIEW_REQUIRED` applies -- confirming the real
decision-maker is a compliance fact to check before any pitch, never
optional. Falls back to a tier-based baseline when no specific signal
applies. Templates live in `config/outreach_angles.json`, fully
editable without code changes.

**Compliance note:** this is a starting point for a human caller to
verify and adapt, never a script to read verbatim, and never a basis
for claiming to know something about the owner that isn't actually in
the record (their motivation, health, marital status, finances beyond
what's recorded, etc.). The dashboard's own modal repeats this caveat
next to every angle it shows.

## Data ownership

Several systems now hold overlapping pieces of the same picture. As of
this Phase 1 reconciliation, here is which one is authoritative for what:

| Data | Owned by | Notes |
|---|---|---|
| Operational lead/contact history (which distress filings exist, accumulated across runs) | `records.json`'s own accumulate-by-`doc_num` mechanism (in `duval_leads_scraper.py`) | The dashboard's only input. `--fresh` rebuilds it from scratch; otherwise every run merges into the existing file. **`records.json` is gitignored -- it only persists as long as the same container/disk does.** |
| Stable property/owner identity, evidence confidence, audit history of every run, export preparation | SQLite (`duval_leads_db.py`), durable via the git-tracked `data/backup.json` | Additive only. Reads the same accumulated `records` list the scraper already builds and layers structured fields on top (`property_id`, `owner_id`, `re_number`, confidence labels) -- it does not filter, reorder, or feed back into what `records.json` contains. |
| Qualified/contacted leads, conversations, follow-ups, deal pipeline | HubSpot (via manual CSV import, Phase 9) | No live API/connector is configured in this environment, so there is no automatic push -- `export_hubspot_csv()` produces an import-ready file. Only records explicitly approved via the dashboard's "Approve for HubSpot" control are ever included -- never the raw scrape -- and that approval only reaches the CSV after `sync_hubspot_approvals.py` runs (see "HubSpot Export & Outreach" above). |
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
