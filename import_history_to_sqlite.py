"""
Explicit migration tool: import the operational lead history (records.json,
or any full-fidelity backup.json export) into the SQLite structured
research/scoring layer.

This is deliberately separate from the scraper's own first-run bootstrap
(duval_leads_db.seed_from_records_json, invoked automatically when no
--backup exists yet) -- this script is for running that same import by
hand, inspecting exactly what it does, and testing it against a disposable
copy of the database before SQLite is ever treated as authoritative for
anything records.json currently owns. It never touches records.json
itself or the operational history mechanism; it only reads from it.

Usage:
    # Test against a throwaway copy first -- always do this before --commit.
    python import_history_to_sqlite.py --records records.json --db /tmp/test_import.db

    # Once verified, run against the real (or a fresh) database file:
    python import_history_to_sqlite.py --records records.json --db data/duval_leads.db
"""

import argparse
import json
import os
import sys

import duval_leads_db as db


def run_import(records_path, db_path, verbose=True):
    if not os.path.exists(records_path):
        print(f"!! {records_path} does not exist -- nothing to import", file=sys.stderr)
        return None

    fresh_db = not os.path.exists(db_path)
    conn = db.init_db(db_path)

    before = {t: conn.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"] for t in db.TABLES}

    with open(records_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    records = payload.get("records", [])

    run_id = db.start_run(conn, run_id=f"import_{os.path.basename(records_path)}",
                           sources_run="manual_import_history_to_sqlite")
    n = db.persist_records(conn, records, run_id)
    db.finish_run(conn, run_id, n, status="ok", notes=f"manual import from {records_path}")

    after = {t: conn.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"] for t in db.TABLES}

    if verbose:
        print(f"{'Fresh database' if fresh_db else 'Existing database'}: {db_path}")
        print(f"Source: {records_path} ({len(records)} records in file)")
        print(f"Records persisted this run: {n}")
        print()
        print(f"{'table':<24}{'before':>10}{'after':>10}{'delta':>10}")
        for t in db.TABLES:
            print(f"{t:<24}{before[t]:>10}{after[t]:>10}{after[t]-before[t]:>10}")

    conn.close()
    return {"records_in_file": len(records), "records_persisted": n, "before": before, "after": after}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--records", default="records.json",
                     help="Source: records.json or a backup.json full export (default records.json)")
    ap.add_argument("--db", required=True,
                     help="Target SQLite file. Point this at a throwaway path first to test the import "
                          "before running it against the real database.")
    args = ap.parse_args()

    result = run_import(args.records, args.db)
    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
