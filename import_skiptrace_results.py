"""
Phase 10: apply a skip-tracing vendor's returned results back into the
SQLite structured layer. The other half of export_skiptrace_csv()'s
round trip -- that export's "Lead ID" column IS the owner_id this script
matches on, so there's no fuzzy name/address matching involved and no
ambiguity about which owner a row updates, unless the vendor's own file
says otherwise (see --ambiguous-column below).

No vendor purchase or upload happens here or anywhere in this codebase --
this only reads a CSV you already have and already paid for, exactly the
same "no vendor API integration" discipline as the rest of this pipeline
(see README's Phase 9/10 notes on what's real vs. not built).

Only currently-BLANK phone_1/phone_2/phone_3/email_1/email_2 fields are
ever filled -- an existing value (however it got there, including a
prior skip trace or manual entry) is never overwritten, so contact
history, suppression flags, outreach history, and any previously-
verified detail all survive untouched. Every field this fills is marked
'unverified' -- skip-traced numbers are unverified until a human checks
them, no exception (see owners.contact_verification_json).

Usage:
    # Test against a throwaway copy first -- always do this before --commit.
    cp data/duval_leads.db /tmp/test_skiptrace.db
    python import_skiptrace_results.py --results vendor_results.csv --db /tmp/test_skiptrace.db --vendor "Vendor Name"

    # Once verified, run against the real database file:
    python import_skiptrace_results.py --results vendor_results.csv --db data/duval_leads.db --vendor "Vendor Name"
"""

import argparse
import csv
import os
import sys

import duval_leads_db as db

DEFAULT_HEADER_ALIASES = {
    "lead_id": ["Lead ID", "lead_id", "ID", "Owner ID"],
    "phone_1": ["Phone 1", "Phone1", "Primary Phone"],
    "phone_2": ["Phone 2", "Phone2"],
    "phone_3": ["Phone 3", "Phone3"],
    "email_1": ["Email 1", "Email1", "Primary Email"],
    "email_2": ["Email 2", "Email2"],
    "ambiguous": ["Ambiguous", "Match Confidence", "Confidence", "Match Type"],
}
# Values in the ambiguous/confidence column that mean "don't trust this
# match" -- anything else in that column is treated as a confident match.
AMBIGUOUS_VALUES = {"low", "uncertain", "no match", "none", "ambiguous", "unverified", "false positive"}


def _resolve_headers(actual_headers, aliases=DEFAULT_HEADER_ALIASES):
    lower_to_actual = {h.strip().lower(): h for h in actual_headers}
    return {
        field: next((lower_to_actual[a.strip().lower()] for a in opts if a.strip().lower() in lower_to_actual), None)
        for field, opts in aliases.items()
    }


def rows_to_results(csv_rows):
    if not csv_rows:
        return []
    resolved = _resolve_headers(csv_rows[0].keys())
    results = []
    for row in csv_rows:
        lead_id_header = resolved.get("lead_id")
        if not lead_id_header:
            continue
        result = {"lead_id": (row.get(lead_id_header) or "").strip()}
        for field in ("phone_1", "phone_2", "phone_3", "email_1", "email_2"):
            header = resolved.get(field)
            if header:
                result[field] = (row.get(header) or "").strip()
        amb_header = resolved.get("ambiguous")
        if amb_header:
            val = (row.get(amb_header) or "").strip().lower()
            result["ambiguous"] = val in AMBIGUOUS_VALUES
        results.append(result)
    return results


def run_import(results_path, db_path, vendor="", source="", verbose=True):
    if not os.path.exists(results_path):
        print(f"!! {results_path} does not exist -- nothing to apply", file=sys.stderr)
        return None
    if not os.path.exists(db_path):
        print(f"!! {db_path} does not exist -- run the scraper or an import first", file=sys.stderr)
        return None

    conn = db.init_db(db_path)
    with open(results_path, "r", encoding="utf-8-sig", newline="") as f:
        csv_rows = list(csv.DictReader(f))
    results = rows_to_results(csv_rows)

    n_applied, n_ambiguous, unmatched = db.apply_skiptrace_results(conn, results, vendor=vendor, source=source)

    if verbose:
        print(f"Database: {db_path}")
        print(f"Source: {results_path} ({len(csv_rows)} rows)")
        print(f"Applied: {n_applied} owner(s) got at least one new phone/email field filled")
        print(f"Ambiguous (flagged, not applied -- queued for manual review instead): {n_ambiguous}")
        if unmatched:
            print(f"Unmatched Lead ID(s) -- no owner on file with this ID (never guessed, just skipped): {len(unmatched)}")
            for u in unmatched[:20]:
                print(f"  - {u}")
            if len(unmatched) > 20:
                print(f"  ... and {len(unmatched) - 20} more")

    conn.close()
    return {"rows_in_file": len(csv_rows), "applied": n_applied, "ambiguous": n_ambiguous, "unmatched": unmatched}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True, help="Vendor's returned skip-trace results CSV")
    ap.add_argument("--db", required=True,
                     help="Target SQLite file. Point this at a throwaway copy first to test the import "
                          "before running it against the real database.")
    ap.add_argument("--vendor", default="", help="Vendor name, recorded on owners.skip_trace_vendor")
    ap.add_argument("--source", default="", help="Free-text source note, recorded on owners.skip_trace_source")
    args = ap.parse_args()

    result = run_import(args.results, args.db, vendor=args.vendor, source=args.source)
    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
