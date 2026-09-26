"""
Phase 10: FSBO intake -- manual CSV capture of Facebook Marketplace /
Zillow For-Sale-By-Owner listings. Neither site is assumed to offer an
accessible scraper API (per the Jacksonville campaign rules, no new
scraper was built for either) -- this is the intake side only: you (or
whoever's browsing FSBO listings) fills in a spreadsheet by hand and this
script loads it, or db.add_fsbo_listing() is called directly for a single
entry (see that function's docstring in duval_leads_db.py).

Applies the campaign's geography/property-type/individual-owner rules via
the same compute_campaign_eligibility() every other source uses -- a FSBO
post very often doesn't name the legal owner at all, so ownership
verification is marked 'review' (pending) rather than assumed individual
just because that's the common case. Dedupes against existing properties
through the normal property_identity() mechanism without erasing source
history: each capture is its own row in fsbo_listings (see that table's
comment in duval_leads_db.py), appended, never overwritten.

Usage:
    # See what your file's actual column headers are:
    python import_fsbo_csv.py --csv my_fsbo_list.csv --dump-headers

    # Dry run -- persists into a throwaway in-memory database only:
    python import_fsbo_csv.py --csv my_fsbo_list.csv --dry-run

    # Test against a throwaway copy first -- always do this before a real run.
    python import_fsbo_csv.py --csv my_fsbo_list.csv --db /tmp/test_fsbo.db

    # Once verified, run against the real database file. --source is only
    # needed if your CSV has no per-row Source column:
    python import_fsbo_csv.py --csv my_fsbo_list.csv --db data/duval_leads.db --source facebook
"""

import argparse
import csv
import json
import os
import sys

import duval_leads_db as db

FIELD_MAP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "fsbo_field_map.json")


def load_field_map(path=FIELD_MAP_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_headers(field_map, actual_headers):
    lower_to_actual = {h.strip().lower(): h for h in actual_headers}
    return {
        field: next((lower_to_actual[a.strip().lower()] for a in aliases if a.strip().lower() in lower_to_actual), None)
        for field, aliases in field_map.items() if not field.startswith("_")
    }


def _get(row, resolved, field, default=""):
    header = resolved.get(field)
    return (row.get(header) or "").strip() if header else default


def _to_float(s):
    try:
        return float(str(s).replace(",", "").replace("$", "")) if s else None
    except ValueError:
        return None


def run_import(csv_path, db_path, field_map_path=FIELD_MAP_PATH, default_source="", dry_run=False, verbose=True):
    if not os.path.exists(csv_path):
        print(f"!! {csv_path} does not exist -- nothing to import", file=sys.stderr)
        return None
    field_map = load_field_map(field_map_path)
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        csv_rows = list(csv.DictReader(f))
    resolved = _resolve_headers(field_map, csv_rows[0].keys()) if csv_rows else {}

    target_db = ":memory:" if dry_run else db_path
    conn = db.init_db(target_db)
    before = conn.execute("SELECT COUNT(*) c FROM fsbo_listings").fetchone()["c"]

    n_ok, n_skipped, results = 0, 0, []
    for i, row in enumerate(csv_rows):
        source = (_get(row, resolved, "source") or default_source).strip().lower()
        prop_address = _get(row, resolved, "prop_address")
        if source not in db.FSBO_VALID_SOURCES:
            n_skipped += 1
            print(f"!! row {i+2}: source {source!r} is not 'facebook' or 'zillow' -- skipped "
                  f"(pass --source or fix the Source column)", file=sys.stderr)
            continue
        if not prop_address:
            n_skipped += 1
            print(f"!! row {i+2}: no property address -- skipped", file=sys.stderr)
            continue
        listing_id, prop_id, eligibility, reason = db.add_fsbo_listing(
            conn, source, prop_address,
            prop_city=_get(row, resolved, "prop_city"),
            prop_state=_get(row, resolved, "prop_state") or "FL",
            prop_zip=_get(row, resolved, "prop_zip"),
            owner_name=_get(row, resolved, "owner"),
            listing_url=_get(row, resolved, "listing_url"),
            asking_price=_to_float(_get(row, resolved, "asking_price")),
            posted_date=_get(row, resolved, "posted_date") or None,
            seller_contact_method=_get(row, resolved, "seller_contact_method"),
            property_facts=_get(row, resolved, "property_facts"),
            follow_up_date=_get(row, resolved, "follow_up_date") or None,
            capture_date=_get(row, resolved, "capture_date") or None,
        )
        n_ok += 1
        results.append((prop_address, eligibility, reason))

    after = conn.execute("SELECT COUNT(*) c FROM fsbo_listings").fetchone()["c"]

    if verbose:
        label = "DRY RUN (in-memory only)" if dry_run else db_path
        print(f"Database: {label}")
        print(f"Source: {csv_path} ({len(csv_rows)} rows -> {n_ok} listings recorded, {n_skipped} skipped)")
        print(f"fsbo_listings rows: {before} -> {after} (delta {after - before})")
        print()
        for addr, elig, reason in results:
            print(f"  [{elig:<22}] {addr} -- {reason}")

    conn.close()
    return {"rows_in_file": len(csv_rows), "recorded": n_ok, "skipped": n_skipped, "results": results}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True, help="FSBO capture CSV to import")
    ap.add_argument("--db", default="data/duval_leads.db",
                     help="Target SQLite file. Point this at a throwaway path first, or use --dry-run.")
    ap.add_argument("--field-map", default=FIELD_MAP_PATH, help="Default config/fsbo_field_map.json")
    ap.add_argument("--source", default="", help="'facebook' or 'zillow' -- applied to every row when the "
                                                    "CSV has no per-row Source column")
    ap.add_argument("--dry-run", action="store_true",
                     help="Persist into a throwaway in-memory database only -- never touches --db.")
    ap.add_argument("--dump-headers", action="store_true")
    args = ap.parse_args()

    if args.dump_headers:
        with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
            headers = next(csv.reader(f))
        print(f"{len(headers)} columns in {args.csv}:")
        for h in headers:
            print(f"  {h!r}")
        return

    result = run_import(args.csv, args.db, field_map_path=args.field_map,
                         default_source=args.source, dry_run=args.dry_run)
    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
