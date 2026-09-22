"""
Phase 9: sync dashboard-approved leads into SQLite before generating the
HubSpot export CSV.

The dashboard's "Approve for HubSpot export" checkbox and "Suppress this
lead" control write straight to the published artifact's own `db`
capability (a `leads/{property_id}` document per property) -- that's a
convenience write surface, not the system of record (see README's "Data
ownership" table), and the standalone scraper process has no browser/JS
context to read it back from directly. This script is the other half of
that loop: it takes a JSON dump of the artifact's `leads` collection
(fetched with the ArtifactData tool -- `action: "query"` or `"list"`
against the published dashboard, collection "leads") and applies it to
SQLite via duval_leads_db.sync_dashboard_leads_state(), so
export_hubspot_csv() actually reflects what was approved in the
dashboard.

Expected JSON shape: a flat object keyed by property_id, each value
shaped exactly like duval_dashboard.html's saveLeadState() writes it --
e.g. {"prop_abc123": {"approved_for_hubspot": true, "approved_by": "...",
"approved_at": "...", "suppressed": false, ...}, ...}. This is the same
shape the dashboard itself builds into LEAD_STATE from
`dbCap.collection("leads").get()`.

Usage:
    # Test against a throwaway copy first -- always do this before --commit.
    python sync_hubspot_approvals.py --leads-json leads_dump.json --db /tmp/test_sync.db

    # Once verified, run against the real database file:
    python sync_hubspot_approvals.py --leads-json leads_dump.json --db data/duval_leads.db
"""

import argparse
import json
import os
import sys

import duval_leads_db as db


def run_sync(leads_json_path, db_path, verbose=True):
    if not os.path.exists(leads_json_path):
        print(f"!! {leads_json_path} does not exist -- nothing to sync", file=sys.stderr)
        return None
    if not os.path.exists(db_path):
        print(f"!! {db_path} does not exist -- run the scraper or an import first", file=sys.stderr)
        return None

    conn = db.init_db(db_path)

    with open(leads_json_path, "r", encoding="utf-8") as f:
        leads_by_property_id = json.load(f)

    before_suppressions = conn.execute("SELECT COUNT(*) c FROM suppressions").fetchone()["c"]
    before_approvals = conn.execute(
        "SELECT COUNT(*) c FROM hubspot_export_flags WHERE approved_by IS NOT NULL AND approved_by != ''"
    ).fetchone()["c"]

    n = db.sync_dashboard_leads_state(conn, leads_by_property_id)

    after_suppressions = conn.execute("SELECT COUNT(*) c FROM suppressions").fetchone()["c"]
    after_approvals = conn.execute(
        "SELECT COUNT(*) c FROM hubspot_export_flags WHERE approved_by IS NOT NULL AND approved_by != ''"
    ).fetchone()["c"]

    if verbose:
        print(f"Database: {db_path}")
        print(f"Source: {leads_json_path} ({len(leads_by_property_id)} dashboard lead docs)")
        print(f"Rows applied this run: {n}")
        print()
        print(f"{'table':<24}{'before':>10}{'after':>10}{'delta':>10}")
        print(f"{'suppressions':<24}{before_suppressions:>10}{after_suppressions:>10}"
              f"{after_suppressions - before_suppressions:>10}")
        print(f"{'hubspot approved':<24}{before_approvals:>10}{after_approvals:>10}"
              f"{after_approvals - before_approvals:>10}")

    conn.close()
    return {"leads_in_file": len(leads_by_property_id), "rows_applied": n,
            "before_approvals": before_approvals, "after_approvals": after_approvals}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--leads-json", required=True,
                     help="JSON dump of the artifact's `leads` collection, keyed by property_id "
                          "(fetch with the ArtifactData tool against the published dashboard).")
    ap.add_argument("--db", required=True,
                     help="Target SQLite file. Point this at a throwaway path first to test the sync "
                          "before running it against the real database.")
    args = ap.parse_args()

    result = run_sync(args.leads_json, args.db)
    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
