"""
Synthetic test fixture for the probate/estate-ownership review-required gap:
review-required detection previously only fired for is_trust=1 owners or a
cat='probate' document, so an owner display_name like "CHEW RUTH ESTATE" --
found live in production data, attached only to ordinary lien/tax documents,
never a probate filing -- passed straight through as if it were a normal,
contactable individual. See duval_leads_db.is_probate_estate_name and its
call sites in persist_records/compute_dealability_score, and
duval_leads_scraper.enrich_records_with_ids.

Same "throwaway db, no network, synthetic records" discipline as every
other phase's verification in this project (see Phase 9's commit message).
Run standalone, prints PASS/FAIL per assertion, exits nonzero on any
failure:

    python test_probate_estate_review.py
"""

import json
import sys

import duval_leads_db as db

PASS = []
FAIL = []


def check(label, condition):
    if condition:
        PASS.append(label)
        print(f"  PASS -- {label}")
    else:
        FAIL.append(label)
        print(f"  FAIL -- {label}")


def test_pattern_matching():
    print("\n[1] is_probate_estate_name() pattern matching")
    # Should be flagged -- real probate/estate language.
    check('"CHEW RUTH ESTATE" is flagged', db.is_probate_estate_name("CHEW RUTH ESTATE"))
    check('"SMITH JOHN DECEASED" is flagged', db.is_probate_estate_name("SMITH JOHN DECEASED"))
    check('"JANE DOE LIFE ESTATE" is flagged', db.is_probate_estate_name("JANE DOE LIFE ESTATE"))
    check('"ESTATE OF JOHN Q PUBLIC" is flagged', db.is_probate_estate_name("ESTATE OF JOHN Q PUBLIC"))
    check('lowercase "chew ruth estate" is flagged (case-insensitive)',
          db.is_probate_estate_name("chew ruth estate"))

    # Should NOT be flagged -- ordinary individuals.
    check('ordinary individual "JOHN SMITH" is NOT flagged', not db.is_probate_estate_name("JOHN SMITH"))
    check('ordinary individual "JANE DOE" is NOT flagged', not db.is_probate_estate_name("JANE DOE"))

    # Should NOT be flagged -- companies with "real estate" in the name.
    check('"TRITEN REAL ESTATE PARTNERS" (company) is NOT flagged',
          not db.is_probate_estate_name("TRITEN REAL ESTATE PARTNERS"))
    check('"ABC REAL ESTATE INVESTMENTS LLC" (company) is NOT flagged',
          not db.is_probate_estate_name("ABC REAL ESTATE INVESTMENTS LLC"))
    check('"COASTAL REAL ESTATE HOLDINGS INC" (company) is NOT flagged',
          not db.is_probate_estate_name("COASTAL REAL ESTATE HOLDINGS INC"))

    # Should NOT be flagged -- plural subdivision/HOA-style names, not a
    # decedent's estate (word-boundary: "ESTATE" != "ESTATES").
    check('subdivision-style plural "GREENWAY ESTATES HOA" is NOT flagged',
          not db.is_probate_estate_name("GREENWAY ESTATES HOA"))

    # Defensive: missing/empty display_name never crashes or false-positives.
    check("empty display_name is NOT flagged", not db.is_probate_estate_name(""))
    check("None display_name is NOT flagged", not db.is_probate_estate_name(None))


def make_record(owner, re_number, prop_address, cat="lien"):
    """Shaped like a record duval_leads_scraper.build_records() would
    produce -- deliberately cat='lien', not 'probate', and no 'TRUST' in
    the owner name, so a positive result here can only come from the new
    display_name check, not the pre-existing is_trust/cat=='probate' paths."""
    return {
        "doc_num": f"DOC-{re_number}", "doc_type": "CLAIM OF LIEN", "filed": "2026-01-15",
        "cat": cat, "cat_label": "Claim of Lien", "owner": owner, "grantee": "SOME CREDITOR LLC",
        "amount": None, "legal": "LOT 1 BLK 1 TEST SUBD",
        "prop_address": prop_address, "prop_city": "JACKSONVILLE", "prop_state": "FL", "prop_zip": "32210",
        "mail_address": "3335 EMAN DR", "mail_city": "JACKSONVILLE", "mail_state": "FL", "mail_zip": "32210",
        "clerk_url": "https://or.duvalclerk.com/search/SearchTypeInstrumentNumber",
        "re_number": re_number, "property_use_code": "0100", "property_type_label": "Single Family",
        "property_type_decision": "include", "year_built": 1985, "building_count": 1,
        "flags": [], "score": 35, "source": "Duval County Clerk -- Official Records",
        "market_value": 150000, "assessed_value": 120000, "taxable_value": 100000,
        "last_sale_date": "2005-06-01", "last_sale_price": 80000, "years_owned": 21,
        "code_violation_confidence": None,
    }


def test_pipeline():
    print("\n[2] End-to-end: persist_records() -> compute_dealability_for_all_properties()")
    conn = db.init_db(":memory:")
    run_id = db.start_run(conn, sources_run="test_probate_estate_review")

    records = [
        make_record("CHEW RUTH ESTATE", "1111110000", "100 ESTATE LN"),
        make_record("JOHN SMITH", "2222220000", "200 ORDINARY ST"),
        make_record("TRITEN REAL ESTATE PARTNERS", "3333330000", "300 COMMERCE WAY"),
    ]
    n = db.persist_records(conn, records, run_id)
    check("all 3 synthetic records persisted", n == 3)
    db.compute_dealability_for_all_properties(conn, run_id)

    prop_id = {
        label: db.property_identity(r["re_number"], r["prop_address"], r["prop_city"], r["prop_zip"])[0]
        for label, r in zip(("estate", "individual", "company"), records)
    }

    def doc_flags(pid):
        row = conn.execute("SELECT flags_json FROM documents WHERE property_id=?", (pid,)).fetchone()
        return set(json.loads(row["flags_json"] or "[]")) if row else set()

    check("estate owner's document is flagged PROBATE_REPRESENTATIVE_REVIEW_REQUIRED",
          "PROBATE_REPRESENTATIVE_REVIEW_REQUIRED" in doc_flags(prop_id["estate"]))
    check("ordinary individual's document is NOT flagged",
          "PROBATE_REPRESENTATIVE_REVIEW_REQUIRED" not in doc_flags(prop_id["individual"]))
    check("real-estate company's document is NOT flagged",
          "PROBATE_REPRESENTATIVE_REVIEW_REQUIRED" not in doc_flags(prop_id["company"]))

    def has_review_task(pid):
        row = conn.execute(
            "SELECT id FROM research_tasks WHERE property_id=? AND task_type='verify_probate_representative' "
            "AND status='open'", (pid,),
        ).fetchone()
        return row is not None

    check("estate owner gets a verify_probate_representative research task", has_review_task(prop_id["estate"]))
    check("ordinary individual gets no such research task", not has_review_task(prop_id["individual"]))
    check("real-estate company gets no such research task", not has_review_task(prop_id["company"]))

    def latest_score(pid):
        return conn.execute(
            "SELECT dealability_reason, confidence_reason, outreach_angle FROM scores "
            "WHERE property_id=? ORDER BY computed_at DESC LIMIT 1", (pid,),
        ).fetchone()

    estate_row = latest_score(prop_id["estate"])
    individual_row = latest_score(prop_id["individual"])
    company_row = latest_score(prop_id["company"])

    check('estate owner\'s dealability reason docks ownership-clarity points for "probate/estate"',
          "probate/estate" in estate_row["dealability_reason"].lower())
    check("individual owner's dealability reason shows a confirmed individual, not review-required",
          "individual owner" in individual_row["dealability_reason"].lower())
    check("real-estate company's dealability reason does not claim probate/estate review",
          "probate/estate" not in company_row["dealability_reason"].lower())

    prefix = db.OUTREACH_ANGLES_CONFIG.get("review_required_prefix", "")
    check("review_required_prefix is configured (sanity check on the fixture itself)", bool(prefix))
    check("estate owner's outreach angle carries the review-required prefix",
          prefix in (estate_row["outreach_angle"] or ""))
    check("individual owner's outreach angle has no review-required prefix",
          prefix not in (individual_row["outreach_angle"] or ""))
    check("real-estate company's outreach angle has no review-required prefix",
          prefix not in (company_row["outreach_angle"] or ""))

    check("estate owner's confidence reason notes a review-flag penalty",
          "review flag" in estate_row["confidence_reason"].lower())
    check("individual owner's confidence reason notes no review-flag penalty",
          "review flag" not in individual_row["confidence_reason"].lower())


def test_scraper_dashboard_backfill():
    print("\n[3] duval_leads_scraper.enrich_records_with_ids() backfill for records.json / the dashboard")
    try:
        import duval_leads_scraper as scraper
    except ImportError as e:
        print(f"  SKIP -- duval_leads_scraper's dependencies aren't installed here ({e}); "
              "the duval_leads_db.py-level checks in [1]/[2] already cover the core fix.")
        return

    conn = db.init_db(":memory:")
    run_id = db.start_run(conn, sources_run="test_probate_estate_review")
    records = [
        make_record("CHEW RUTH ESTATE", "4444440000", "400 ESTATE LN"),
        make_record("JOHN SMITH", "5555550000", "500 ORDINARY ST"),
        make_record("TRITEN REAL ESTATE PARTNERS", "6666660000", "600 COMMERCE WAY"),
    ]
    db.persist_records(conn, records, run_id)
    db.compute_dealability_for_all_properties(conn, run_id)
    enriched = scraper.enrich_records_with_ids(conn, records, run_id=run_id)
    by_owner = {r["owner"]: r for r in enriched}

    check("estate owner's records.json flags include PROBATE_REPRESENTATIVE_REVIEW_REQUIRED "
          "(what the dashboard's Review Required Only filter reads)",
          "PROBATE_REPRESENTATIVE_REVIEW_REQUIRED" in by_owner["CHEW RUTH ESTATE"].get("flags", []))
    check("ordinary individual's records.json flags do not",
          "PROBATE_REPRESENTATIVE_REVIEW_REQUIRED" not in by_owner["JOHN SMITH"].get("flags", []))
    check("real-estate company's records.json flags do not",
          "PROBATE_REPRESENTATIVE_REVIEW_REQUIRED" not in by_owner["TRITEN REAL ESTATE PARTNERS"].get("flags", []))


def main():
    test_pattern_matching()
    test_pipeline()
    test_scraper_dashboard_backfill()

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("\nFAILED:")
        for f in FAIL:
            print(f"  - {f}")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
