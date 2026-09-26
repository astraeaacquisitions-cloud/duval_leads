"""
Phase 10: import a PropWire CSV export into the SQLite structured
research/scoring layer, applying the Jacksonville/Duval campaign rules
(geo-fence, single-family focus, individual-owner-only gate).

UNVALIDATED against a real PropWire export -- no sample file was
available when this was written. config/propwire_field_map.json lists
the header spellings this importer looks for; if your file's headers
don't match, run --dump-headers first and add the real spelling to the
matching list in that config file (no code change needed).

This reuses the exact same persist_records() the primary Duval Clerk
pipeline calls -- property_identity()/owner_identity() (parcel/RE#
first, normalized-address fallback) handle dedup automatically, so
re-importing the same PropWire export twice does not create duplicate
properties or owners; it just refreshes last_seen_at. Multiple
properties belonging to the same owner are preserved as-is (each row is
its own property); "avoid duplicate outreach to that owner" is a call-
list-generation concern, not an import-time exclusion -- see README's
Phase 10 section.

Each PropWire row becomes one synthetic 'PROPWIRE_IMPORT' document (cat
'propwire_import', not a distress category) per owner on the row (an
Owner + a Co-Owner become two separate ownerships on the same property,
per campaign rule #2's "multiple individual co-owners are allowed").
Owner-type classification uses the row's own Owner Type column when
present (authoritative) and falls back to name-parsing otherwise -- see
classify_owner_type() in duval_leads_db.py.

Usage:
    # See what your file's actual column headers are:
    python import_propwire.py --csv my_propwire_export.csv --dump-headers

    # Dry run -- persists into a throwaway in-memory database only, never
    # touches --db, and prints the included/excluded/review breakdown:
    python import_propwire.py --csv my_propwire_export.csv --dry-run

    # Test against a throwaway copy first -- always do this before a real run.
    python import_propwire.py --csv my_propwire_export.csv --db /tmp/test_propwire.db

    # Once verified, run against the real database file:
    python import_propwire.py --csv my_propwire_export.csv --db data/duval_leads.db
"""

import argparse
import csv
import json
import os
import sys

import duval_leads_db as db

FIELD_MAP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "propwire_field_map.json")


def load_field_map(path=FIELD_MAP_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_headers(field_map, actual_headers):
    """Returns {our_field: actual_header_or_None}, matching case-
    insensitively and whitespace-trimmed, first alias in the config list
    that appears in the file wins."""
    lower_to_actual = {h.strip().lower(): h for h in actual_headers}
    resolved = {}
    for our_field, aliases in field_map.items():
        if our_field.startswith("_"):
            continue
        resolved[our_field] = next(
            (lower_to_actual[a.strip().lower()] for a in aliases if a.strip().lower() in lower_to_actual),
            None,
        )
    return resolved


def _get(row, resolved, our_field, default=""):
    header = resolved.get(our_field)
    if not header:
        return default
    return (row.get(header) or "").strip()


def _owner_record(row, resolved, owner_field, first_field, last_field):
    """Builds one owner name string for one co-owner slot, preferring a
    combined name field over first+last when both are present."""
    name = _get(row, resolved, owner_field)
    if not name:
        first = _get(row, resolved, first_field)
        last = _get(row, resolved, last_field)
        name = f"{first} {last}".strip()
    return name


def rows_to_records(csv_rows, field_map, source_label="PropWire Export Import"):
    """Adapts PropWire CSV rows into this pipeline's own record-dict shape
    (the same shape build_records() produces), ready for persist_records().
    Returns (records, warnings) -- warnings are rows with no property
    address AND no parcel number at all (nothing to dedup or geo-check on),
    which are skipped rather than guessed into a property."""
    if not csv_rows:
        return [], []
    resolved = _resolve_headers(field_map, csv_rows[0].keys())
    records, warnings = [], []
    for i, row in enumerate(csv_rows):
        prop_address = _get(row, resolved, "prop_address")
        re_number = _get(row, resolved, "re_number")
        if not prop_address and not re_number:
            warnings.append(f"row {i+2}: no property address and no parcel/APN -- skipped (nothing to key on)")
            continue

        shared = {
            "doc_type": "PROPWIRE_IMPORT",
            "cat": "propwire_import",
            "cat_label": "PropWire Lead Import",
            "filed": _get(row, resolved, "last_sale_date") or "",
            "amount": None,
            "legal": "",
            "prop_address": prop_address,
            "prop_city": _get(row, resolved, "prop_city"),
            "prop_state": _get(row, resolved, "prop_state") or "FL",
            "prop_zip": _get(row, resolved, "prop_zip"),
            "mail_address": _get(row, resolved, "mail_address"),
            "mail_city": _get(row, resolved, "mail_city"),
            "mail_state": _get(row, resolved, "mail_state"),
            "mail_zip": _get(row, resolved, "mail_zip"),
            "clerk_url": "",
            "re_number": re_number,
            "flags": ["PROPWIRE_IMPORT"],
            "score": 0,
            "source": source_label,
            "lead_source": "propwire",
            "market_value": _to_float(_get(row, resolved, "market_value")),
            "assessed_value": _to_float(_get(row, resolved, "assessed_value")),
            "taxable_value": _to_float(_get(row, resolved, "taxable_value")),
            "last_sale_date": _get(row, resolved, "last_sale_date") or None,
            "last_sale_price": _to_float(_get(row, resolved, "last_sale_price")),
            "years_owned": _to_int(_get(row, resolved, "years_owned")),
            "property_use_code": _get(row, resolved, "property_use_code")
                                  or _infer_dor_code_from_label(_get(row, resolved, "property_type_label")),
            "property_type_label": _get(row, resolved, "property_type_label"),
            "property_type_decision": "",
            "year_built": _to_int(_get(row, resolved, "year_built")),
            "building_count": _to_int(_get(row, resolved, "building_count")),
            "owner_type": _get(row, resolved, "owner_type") or None,
        }

        owner_1 = _owner_record(row, resolved, "owner", "owner_first_name", "owner_last_name")
        doc_num_key = re_number or prop_address
        if owner_1:
            rec = dict(shared)
            rec["owner"] = owner_1
            rec["doc_num"] = f"PROPWIRE-{doc_num_key}-1"
            records.append(rec)

        owner_2 = _get(row, resolved, "co_owner")
        if owner_2:
            rec = dict(shared)
            rec["owner"] = owner_2
            rec["doc_num"] = f"PROPWIRE-{doc_num_key}-2"
            records.append(rec)

        if not owner_1 and not owner_2:
            rec = dict(shared)
            rec["owner"] = ""
            rec["doc_num"] = f"PROPWIRE-{doc_num_key}-1"
            records.append(rec)
            warnings.append(f"row {i+2} ({prop_address or re_number}): no owner name column matched -- "
                             f"persisted with an empty owner (will classify 'unknown', goes to review).")
    return records, warnings


def _infer_dor_code_from_label(label):
    """PropWire's own free-text 'Property Type' column (not a Florida DOR
    numeric use code -- that's Duval-PAO-specific) sometimes already says
    'Single Family Residential'/'SFR' outright, typically because the user
    already filtered their PropWire search to that property type before
    exporting. When it does, infer the DOR prefix '01' (Single Family) so
    compute_campaign_eligibility's property-type gate doesn't send an
    obviously-single-family PropWire row to manual review just because
    Duval's own use-code field is naturally absent from a non-Duval-PAO
    source. The original label text is still stored as property_type_label
    either way -- this only fills property_use_code, never overwrites or
    hides what PropWire actually said."""
    if not label:
        return ""
    if any(k in label.lower() for k in ("single family", "single-family", "sfr")):
        return "0100"
    return ""


def _to_float(s):
    try:
        return float(str(s).replace(",", "").replace("$", "")) if s else None
    except ValueError:
        return None


def _to_int(s):
    try:
        return int(float(s)) if s else None
    except ValueError:
        return None


def _eligibility_breakdown(conn):
    rows = conn.execute(
        "SELECT campaign_eligibility, COUNT(*) c FROM properties GROUP BY campaign_eligibility"
    ).fetchall()
    return {r["campaign_eligibility"] or "(uncomputed)": r["c"] for r in rows}


def run_import(csv_path, db_path, field_map_path=FIELD_MAP_PATH, dry_run=False, verbose=True):
    if not os.path.exists(csv_path):
        print(f"!! {csv_path} does not exist -- nothing to import", file=sys.stderr)
        return None
    field_map = load_field_map(field_map_path)
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        csv_rows = list(csv.DictReader(f))

    records, warnings = rows_to_records(csv_rows, field_map)

    target_db = ":memory:" if dry_run else db_path
    fresh_db = dry_run or not os.path.exists(db_path)
    conn = db.init_db(target_db)

    before = {t: conn.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"] for t in db.TABLES}

    run_id = db.start_run(conn, run_id=f"import_propwire_{os.path.basename(csv_path)}",
                           sources_run="manual_import_propwire")
    n = db.persist_records(conn, records, run_id)
    db.finish_run(conn, run_id, n, status="ok", notes=f"manual PropWire import from {csv_path}")
    db.compute_dealability_for_all_properties(conn, run_id)

    after = {t: conn.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"] for t in db.TABLES}
    breakdown = _eligibility_breakdown(conn)

    if verbose:
        label = "DRY RUN (in-memory only -- nothing written to --db)" if dry_run else \
                 ("Fresh database" if fresh_db else "Existing database")
        print(f"{label}: {db_path if not dry_run else '(discarded)'}")
        print(f"Source: {csv_path} ({len(csv_rows)} CSV rows -> {len(records)} owner records)")
        print(f"Records persisted this run: {n}")
        if warnings:
            print(f"\n{len(warnings)} row(s) flagged during adaptation:")
            for w in warnings[:20]:
                print(f"  - {w}")
            if len(warnings) > 20:
                print(f"  ... and {len(warnings) - 20} more")
        print()
        print(f"{'table':<24}{'before':>10}{'after':>10}{'delta':>10}")
        for t in db.TABLES:
            print(f"{t:<24}{before[t]:>10}{after[t]:>10}{after[t]-before[t]:>10}")
        print()
        print("Campaign eligibility breakdown (across ALL properties now on file, not just this import):")
        for status, count in sorted(breakdown.items(), key=lambda kv: -kv[1]):
            print(f"  {status:<24}{count:>6}")

    conn.close()
    return {"csv_rows": len(csv_rows), "records_built": len(records), "records_persisted": n,
            "warnings": warnings, "before": before, "after": after, "eligibility_breakdown": breakdown}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", required=True, help="PropWire CSV export to import")
    ap.add_argument("--db", default="data/duval_leads.db",
                     help="Target SQLite file. Point this at a throwaway path first to test the import "
                          "before running it against the real database (or use --dry-run).")
    ap.add_argument("--field-map", default=FIELD_MAP_PATH,
                     help="Field-mapping config (default config/propwire_field_map.json)")
    ap.add_argument("--dry-run", action="store_true",
                     help="Persist into a throwaway in-memory database only -- never touches --db -- "
                          "and print the included/excluded/review breakdown.")
    ap.add_argument("--dump-headers", action="store_true",
                     help="Print this CSV's actual header row and exit, to help validate/adjust --field-map.")
    args = ap.parse_args()

    if args.dump_headers:
        with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
            headers = next(csv.reader(f))
        print(f"{len(headers)} columns in {args.csv}:")
        for h in headers:
            print(f"  {h!r}")
        return

    result = run_import(args.csv, args.db, field_map_path=args.field_map, dry_run=args.dry_run)
    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
