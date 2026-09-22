"""
Duval County Lead Tracker -- structured research/scoring layer (Phase 1)
--------------------------------------------------------------------------

DATA OWNERSHIP (who owns what, as of Phase 1):
  - records.json's own accumulate-by-doc_num history (see the scraper's
    module docstring) remains the operational lead/contact history and
    the sole input the dashboard reads. This module does not replace,
    reset, or write back into that mechanism -- it reads the same
    `records` list the scraper already builds, after history has been
    merged in, and layers structured research on top of it.
  - SQLite (this module) owns: stable property/owner identity, evidence
    provenance and confidence, audit history of every run, and export
    preparation (CSV / HubSpot). It is rebuilt on every run from the
    git-tracked `data/backup.json` full export (or, on the very first
    run, seeded from the legacy records.json snapshot) so the database
    file itself never has to be the only copy of anything -- the
    ephemeral container it runs in is not a safe place to keep data
    permanently.
  - HubSpot (future, Phase 9) will own qualified/contacted leads,
    conversations, follow-ups, and deal pipeline once a record is
    explicitly approved for export -- never the raw scrape.
  - CSV exports (`export_csv`, `export_hubspot_csv`, `export_backup_json`)
    are portable backup/recovery and manual-editing copies, regenerable
    from SQLite at any time.

SQLite is explicitly NOT authoritative yet: it is additive and
backward-compatible, sitting alongside records.json's own history
mechanism rather than replacing it. `import_history_to_sqlite.py`
provides an explicit, separately-run migration path for the day this
does become authoritative -- that migration is not automatic and must
be tested against a copy of the data first.

Identity model
--------------
Property and owner IDs are *deterministic hashes*, not autoincrement
counters, so re-running the pipeline against the same underlying facts
always produces the same IDs (idempotent) and so dedup falls out of the
hashing scheme instead of needing a separate merge step for the common
case:

  property_id: parcel RE# when known (strong key), else a hash of the
  normalized situs address (weak key -- two different parcels could in
  theory normalize to the same string, which is why the confidence is
  recorded, not just the ID).

  owner_id: name + mailing address when both are known (the only case
  where two records can be safely assumed to be the same person/entity).
  When the mailing address is missing -- true for ~50% of today's
  enrichment -- collapsing by name alone would risk silently merging two
  different people who happen to share a name, so the key falls back to
  name + property (still a meaningful dedup axis: the same name showing
  up repeatedly against the same property is almost certainly the same
  owner) and, failing that, to name + document (i.e. left unmerged --
  under-merging is a research task later; over-merging corrupts identity
  data silently and is much worse). This is deliberately conservative,
  per the Phase 1 decision to not auto-merge ambiguous owners.

Nothing in this module talks to the network. It only reads/writes the
local SQLite file and the JSON/CSV export files.
"""

import csv
import datetime
import hashlib
import json
import os
import re
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    id TEXT PRIMARY KEY,
    re_number TEXT,
    situs_address TEXT,
    situs_city TEXT,
    situs_state TEXT,
    situs_zip TEXT,
    address_match_confidence TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT,
    merged_into_id TEXT,
    -- Phase 2: Florida DOR property use classification (see
    -- classify_property_type() in duval_leads_scraper.py). decision is
    -- 'include' | 'review' | 'exclude'; excluded records never reach this
    -- table at all (filtered in build_records()'s keep()), so any row
    -- seen here is 'include' or 'review' by construction, or NULL for
    -- records predating this feature.
    property_use_code TEXT,
    property_type_label TEXT,
    property_type_decision TEXT,
    year_built INTEGER,
    building_count INTEGER
);

CREATE TABLE IF NOT EXISTS owners (
    id TEXT PRIMARY KEY,
    display_name TEXT,
    owner_type TEXT,
    mailing_address TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    mailing_zip TEXT,
    is_entity INTEGER,
    identity_confidence TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT,
    merged_into_id TEXT,
    -- Trust ownership (see is_trust_owned): a recorded trust is a
    -- legitimate motivated-seller lead, not a disqualifier, but nobody
    -- assumes the trustee -- let alone a beneficiary, relative, or
    -- occupant -- has authority to sell without confirming it.
    is_trust INTEGER DEFAULT 0,
    trust_name TEXT,
    trust_type TEXT,
    trustee_name TEXT,
    trustee_mailing_address TEXT,
    trustee_authority_verified TEXT DEFAULT 'unknown',
    contact_identity_verified TEXT DEFAULT 'unknown',
    additional_title_review_required TEXT DEFAULT 'unknown',
    notes TEXT
);

CREATE TABLE IF NOT EXISTS ownerships (
    property_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    is_current INTEGER DEFAULT 1,
    first_seen_at TEXT,
    last_seen_at TEXT,
    deed_recording_date TEXT,
    ownership_source TEXT,
    PRIMARY KEY (property_id, owner_id)
);

CREATE TABLE IF NOT EXISTS documents (
    id TEXT PRIMARY KEY,
    doc_num TEXT,
    doc_type TEXT,
    cat TEXT,
    cat_label TEXT,
    filed_date TEXT,
    owner_name TEXT,
    other_party TEXT,
    legal_desc TEXT,
    amount TEXT,
    property_id TEXT,
    owner_id TEXT,
    -- Phase 6: STRONG (filer literally names code enforcement/compliance)
    -- or MODERATE (a broader municipal-filer match -- could be a
    -- demolition/nuisance/utility lien instead), only set for cat=
    -- 'code_violation' documents. Never a claim of confirmed case status.
    code_violation_confidence TEXT,
    flags_json TEXT,
    legacy_score INTEGER,
    source_name TEXT,
    source_url TEXT,
    scrape_run_id TEXT,
    raw_json TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT
);

CREATE TABLE IF NOT EXISTS evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    value TEXT,
    confidence_status TEXT NOT NULL,
    source_name TEXT,
    source_url TEXT,
    observed_at TEXT,
    is_current INTEGER DEFAULT 1,
    superseded_by_id INTEGER
);

CREATE TABLE IF NOT EXISTS suppressions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    note TEXT,
    created_by TEXT,
    created_at TEXT,
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id TEXT,
    property_id TEXT,
    channel TEXT,
    occurred_at TEXT,
    outcome TEXT,
    note TEXT,
    created_by TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS overrides (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    created_by TEXT,
    reason TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS research_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id TEXT,
    owner_id TEXT,
    document_id TEXT,
    task_type TEXT NOT NULL,
    description TEXT,
    status TEXT DEFAULT 'open',
    priority TEXT,
    created_at TEXT,
    completed_at TEXT,
    result_note TEXT
);

CREATE TABLE IF NOT EXISTS hubspot_export_flags (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    approved_by TEXT,
    approved_at TEXT,
    exported_at TEXT,
    hubspot_record_id TEXT,
    PRIMARY KEY (entity_type, entity_id)
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id TEXT PRIMARY KEY,
    started_at TEXT,
    finished_at TEXT,
    sources_run TEXT,
    records_in INTEGER,
    records_out INTEGER,
    status TEXT,
    notes TEXT
);

-- Phase 3: value/equity. Every value here is explicitly labeled --
-- pao_assessed_value and pao_market_value are the two distinct county
-- tax-roll figures (never treated as interchangeable, never as a
-- verified current market value); automated_estimated_value,
-- estimated_asis_market_value, estimated_arv, and comparable_sales_used
-- stay NULL until a paid AVM/comp engine exists (a later phase) --
-- deliberately not backfilled with a PAO value under a different name.
CREATE TABLE IF NOT EXISTS valuations (
    property_id TEXT PRIMARY KEY,
    pao_assessed_value REAL,
    pao_market_value REAL,
    pao_taxable_value REAL,
    automated_estimated_value REAL,
    estimated_asis_market_value REAL,
    estimated_arv REAL,
    value_confidence TEXT,
    valuation_date TEXT,
    valuation_source TEXT,
    comparable_sales_used TEXT,
    last_sale_date TEXT,
    last_sale_price REAL,
    years_owned INTEGER,
    active_lien_count INTEGER,
    has_tax_distress INTEGER,
    mortgage_estimate TEXT DEFAULT 'UNKNOWN',
    equity_signal TEXT,
    equity_confidence TEXT,
    equity_reasoning TEXT,
    tax_deed_stage TEXT,
    days_until_tax_sale INTEGER,
    foreclosure_stage TEXT,
    days_since_foreclosure_milestone INTEGER,
    code_enforcement_stage TEXT,
    code_violation_count INTEGER,
    absentee_stage TEXT,
    landlord_portfolio_stage TEXT,
    portfolio_property_count INTEGER,
    portfolio_distressed_count INTEGER,
    computed_at TEXT
);

-- One row per scoring run per property (append-only -- this IS the audit
-- log; the dashboard/export layer reads the latest row per property_id).
-- Only dealability_score is populated as of Phase 3; the other four
-- scores and tier arrive in Phase 8's stacked-distress/contact-priority
-- engine, which is why they're nullable rather than added later.
CREATE TABLE IF NOT EXISTS scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id TEXT NOT NULL,
    computed_at TEXT,
    distress_score INTEGER,
    urgency_score INTEGER,
    dealability_score INTEGER,
    dealability_reason TEXT,
    confidence_score INTEGER,
    contact_priority_score INTEGER,
    tier TEXT,
    tier_reason TEXT,
    weights_version TEXT,
    scrape_run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_documents_property ON documents(property_id);
CREATE INDEX IF NOT EXISTS idx_documents_owner ON documents(owner_id);
CREATE INDEX IF NOT EXISTS idx_scores_property ON scores(property_id, computed_at);
CREATE INDEX IF NOT EXISTS idx_evidence_entity ON evidence(entity_type, entity_id, field_name);
CREATE INDEX IF NOT EXISTS idx_ownerships_owner ON ownerships(owner_id);
"""

TABLES = [
    "properties", "owners", "ownerships", "documents", "evidence",
    "suppressions", "contacts", "overrides", "research_tasks",
    "hubspot_export_flags", "scrape_runs", "valuations", "scores",
]


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _norm(s):
    """Minimal, dependency-free normalization for ID hashing -- not the
    fuzzy matcher used upstream for owner disambiguation, just enough to
    make trivial formatting differences (case, punctuation, extra spaces)
    hash identically."""
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _hash_id(prefix, *parts):
    key = "|".join(_norm(p) for p in parts)
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def property_identity(re_number, situs_address, situs_city, situs_zip):
    """Returns (property_id, confidence) or (None, 'unmatched')."""
    re_clean = re.sub(r"[^0-9]", "", re_number or "")
    if re_clean:
        return _hash_id("prop", "RE", re_clean), "verified_parcel"
    if situs_address:
        return _hash_id("prop", "ADDR", situs_address, situs_city, situs_zip), "address_only"
    return None, "unmatched"


def owner_identity(display_name, mailing_address, mailing_city, mailing_zip, property_id, doc_id):
    """Returns (owner_id, confidence). Deliberately conservative -- see
    module docstring. Falls back to per-document identity (i.e.
    effectively unmerged) rather than ever risk merging two different
    people who share a name."""
    if not display_name:
        return None, "unmatched"
    if mailing_address:
        return _hash_id("own", display_name, mailing_address, mailing_city, mailing_zip), "name_and_mailing_address"
    if property_id:
        return _hash_id("own", display_name, "PROP", property_id), "name_and_property_only"
    return _hash_id("own", display_name, "DOC", doc_id), "name_only_unmerged"


# Columns added to a table AFTER its original CREATE TABLE. "CREATE TABLE
# IF NOT EXISTS" is a no-op on a table that already exists -- it does NOT
# add new columns -- so a database file created before one of these was
# added would otherwise crash the first time a query touches it, on any
# container whose disk survives a schema change between runs. Append here
# whenever a future phase adds a column to an existing table; each entry
# is (table, column, "TYPE [DEFAULT ...]") exactly as it would appear in
# CREATE TABLE, applied via ALTER TABLE ... ADD COLUMN, guarded by
# checking PRAGMA table_info first so this is safe to run every startup.
SCHEMA_MIGRATIONS = [
    ("properties", "property_use_code", "TEXT"),
    ("properties", "property_type_label", "TEXT"),
    ("properties", "property_type_decision", "TEXT"),
    ("properties", "year_built", "INTEGER"),
    ("properties", "building_count", "INTEGER"),
    ("valuations", "tax_deed_stage", "TEXT"),
    ("valuations", "days_until_tax_sale", "INTEGER"),
    ("valuations", "foreclosure_stage", "TEXT"),
    ("valuations", "days_since_foreclosure_milestone", "INTEGER"),
    ("documents", "code_violation_confidence", "TEXT"),
    ("valuations", "code_enforcement_stage", "TEXT"),
    ("valuations", "code_violation_count", "INTEGER"),
    ("valuations", "absentee_stage", "TEXT"),
    ("valuations", "landlord_portfolio_stage", "TEXT"),
    ("valuations", "portfolio_property_count", "INTEGER"),
    ("valuations", "portfolio_distressed_count", "INTEGER"),
]


def _migrate_schema(conn):
    for table, column, type_decl in SCHEMA_MIGRATIONS:
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_decl}")
    conn.commit()


def init_db(path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    _migrate_schema(conn)
    return conn


def is_empty(conn):
    row = conn.execute("SELECT COUNT(*) AS n FROM documents").fetchone()
    return row["n"] == 0


# ---------------------------------------------------------------------------
# Rehydration: load the last git-committed backup, or (first run only)
# seed from the legacy flat records.json snapshot.
# ---------------------------------------------------------------------------

def load_backup_into_db(conn, backup_path):
    with open(backup_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    cur = conn.cursor()
    for table in TABLES:
        rows = data.get(table, [])
        if not rows:
            continue
        cols = list(rows[0].keys())
        placeholders = ",".join("?" for _ in cols)
        col_list = ",".join(cols)
        cur.executemany(
            f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})",
            [tuple(r.get(c) for c in cols) for r in rows],
        )
    conn.commit()
    return sum(len(data.get(t, [])) for t in TABLES)


def seed_from_records_json(conn, records_json_path, run_id="legacy_seed"):
    """One-time bootstrap for the very first Phase 1 run: import whatever
    is currently in records.json (today's flat, unpersisted snapshot) so
    no existing lead is lost when the database is introduced."""
    with open(records_json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    records = payload.get("records", [])
    start_run(conn, run_id=run_id, sources_run="legacy_records_json_seed")
    n = persist_records(conn, records, run_id, source_note="legacy seed")
    finish_run(conn, run_id, records_out=n, status="ok",
               notes=f"seeded from {records_json_path}")
    return n


# ---------------------------------------------------------------------------
# Run bookkeeping
# ---------------------------------------------------------------------------

def start_run(conn, run_id=None, sources_run=""):
    run_id = run_id or f"run_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    conn.execute(
        "INSERT OR REPLACE INTO scrape_runs (id, started_at, sources_run, status) VALUES (?, ?, ?, ?)",
        (run_id, now_iso(), sources_run, "running"),
    )
    conn.commit()
    return run_id


def finish_run(conn, run_id, records_out, status="ok", notes=""):
    conn.execute(
        "UPDATE scrape_runs SET finished_at=?, records_out=?, status=?, notes=? WHERE id=?",
        (now_iso(), records_out, status, notes, run_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Persisting scraped records
# ---------------------------------------------------------------------------

def _doc_id(source, doc_num, doc_type):
    return _hash_id("doc", source, doc_num, doc_type)


def _upsert_property(conn, ts, re_number, situs_address, situs_city, situs_state, situs_zip,
                      property_use_code="", property_type_label="", property_type_decision="",
                      year_built=None, building_count=None):
    prop_id, confidence = property_identity(re_number, situs_address, situs_city, situs_zip)
    if not prop_id:
        return None, "unmatched"
    row = conn.execute("SELECT id FROM properties WHERE id=?", (prop_id,)).fetchone()
    if row:
        conn.execute(
            "UPDATE properties SET re_number=COALESCE(NULLIF(?,''), re_number), "
            "situs_address=COALESCE(NULLIF(?,''), situs_address), "
            "situs_city=COALESCE(NULLIF(?,''), situs_city), "
            "situs_state=COALESCE(NULLIF(?,''), situs_state), "
            "situs_zip=COALESCE(NULLIF(?,''), situs_zip), "
            "address_match_confidence=?, last_seen_at=?, "
            "property_use_code=COALESCE(NULLIF(?,''), property_use_code), "
            "property_type_label=COALESCE(NULLIF(?,''), property_type_label), "
            "property_type_decision=COALESCE(NULLIF(?,''), property_type_decision), "
            "year_built=COALESCE(?, year_built), building_count=COALESCE(?, building_count) "
            "WHERE id=?",
            (re_number, situs_address, situs_city, situs_state, situs_zip, confidence, ts,
             property_use_code, property_type_label, property_type_decision,
             year_built, building_count, prop_id),
        )
    else:
        conn.execute(
            "INSERT INTO properties (id, re_number, situs_address, situs_city, situs_state, "
            "situs_zip, address_match_confidence, first_seen_at, last_seen_at, "
            "property_use_code, property_type_label, property_type_decision, year_built, building_count) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (prop_id, re_number, situs_address, situs_city, situs_state, situs_zip, confidence, ts, ts,
             property_use_code, property_type_label, property_type_decision, year_built, building_count),
        )
    return prop_id, confidence


def _upsert_valuation_raw(conn, ts, property_id, market_value, assessed_value, taxable_value,
                           last_sale_date, last_sale_price, years_owned):
    """Stores only the raw PAO figures as they're scraped -- COALESCE onto
    whatever's already there so a record with a missing field doesn't blank
    out a previously-known one. Equity signal / dealability are computed
    separately, once per property per run, by compute_dealability() below
    -- they depend on cross-referencing every document tied to the
    property, not just the one this record came from."""
    if not property_id:
        return
    row = conn.execute("SELECT property_id FROM valuations WHERE property_id=?", (property_id,)).fetchone()
    if row:
        conn.execute(
            "UPDATE valuations SET pao_market_value=COALESCE(?, pao_market_value), "
            "pao_assessed_value=COALESCE(?, pao_assessed_value), "
            "pao_taxable_value=COALESCE(?, pao_taxable_value), "
            "last_sale_date=COALESCE(NULLIF(?,''), last_sale_date), "
            "last_sale_price=COALESCE(?, last_sale_price), "
            "years_owned=COALESCE(?, years_owned), "
            "valuation_date=?, valuation_source='Duval County Property Appraiser' "
            "WHERE property_id=?",
            (market_value, assessed_value, taxable_value, last_sale_date or "",
             last_sale_price, years_owned, ts, property_id),
        )
    else:
        conn.execute(
            "INSERT INTO valuations (property_id, pao_market_value, pao_assessed_value, "
            "pao_taxable_value, last_sale_date, last_sale_price, years_owned, "
            "valuation_date, valuation_source, mortgage_estimate) "
            "VALUES (?,?,?,?,?,?,?,?,?,'UNKNOWN')",
            (property_id, market_value, assessed_value, taxable_value, last_sale_date,
             last_sale_price, years_owned, ts, "Duval County Property Appraiser"),
        )


def _upsert_owner(conn, ts, display_name, mailing_address, mailing_city, mailing_state,
                   mailing_zip, is_entity, is_trust, property_id, doc_id):
    owner_id, confidence = owner_identity(display_name, mailing_address, mailing_city,
                                           mailing_zip, property_id, doc_id)
    if not owner_id:
        return None, "unmatched"
    row = conn.execute("SELECT id FROM owners WHERE id=?", (owner_id,)).fetchone()
    if row:
        conn.execute(
            "UPDATE owners SET mailing_address=COALESCE(NULLIF(?,''), mailing_address), "
            "mailing_city=COALESCE(NULLIF(?,''), mailing_city), "
            "mailing_state=COALESCE(NULLIF(?,''), mailing_state), "
            "mailing_zip=COALESCE(NULLIF(?,''), mailing_zip), "
            "is_entity=?, is_trust=CASE WHEN ? THEN 1 ELSE is_trust END, "
            "identity_confidence=?, last_seen_at=? WHERE id=?",
            (mailing_address, mailing_city, mailing_state, mailing_zip,
             int(is_entity), int(is_trust), confidence, ts, owner_id),
        )
    else:
        conn.execute(
            "INSERT INTO owners (id, display_name, owner_type, mailing_address, mailing_city, "
            "mailing_state, mailing_zip, is_entity, is_trust, identity_confidence, first_seen_at, last_seen_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (owner_id, display_name, "entity" if is_entity else "unknown", mailing_address,
             mailing_city, mailing_state, mailing_zip, int(is_entity), int(is_trust), confidence, ts, ts),
        )
    return owner_id, confidence


def persist_records(conn, records, run_id, source_note=""):
    """Upserts every scraped record into properties/owners/ownerships/
    documents/evidence. Idempotent: re-running against the same document
    (same source + doc_num + doc_type) updates last_seen_at rather than
    duplicating -- this is what makes "first seen" a meaningful signal
    instead of the old always-true "new this week" flag."""
    ts = now_iso()
    n = 0
    for r in records:
        source = r.get("source", "")
        doc_num = r.get("doc_num", "")
        doc_type = r.get("doc_type", "")
        doc_id = _doc_id(source, doc_num, doc_type)

        owner_name = r.get("owner", "") or ""
        # LAND TRUST counts as entity-like (investors use it like an LLC to
        # hold title anonymously); a bare "<Name> Trust" does not -- that's
        # very often an individual's living/family/revocable estate plan,
        # a legitimate seller, not an entity to exclude. See
        # TRUST_OWNERSHIP_REVIEW_REQUIRED in the scraper's compute_score_flags
        # for why trust ownership is flagged for authority verification
        # rather than filtered out.
        is_entity = bool(re.search(
            r"LAND\s*TRUST|\bLLC\b|\bINC\b|\bCORP|\bLP\b|\bLLP\b|\bLTD\b|\bCOMPANY\b|"
            r"\bHOLDINGS\b|\bENTERPRISES\b|\bINVESTMENTS?\b|\bPROPERTIES\b|\bCAPITAL\b|\bVENTURES?\b",
            owner_name, re.I,
        ))
        is_trust = bool(re.search(r"\bTRUST\b", owner_name, re.I))

        prop_id, prop_conf = _upsert_property(
            conn, ts, r.get("re_number", ""), r.get("prop_address", ""),
            r.get("prop_city", ""), r.get("prop_state", ""), r.get("prop_zip", ""),
            r.get("property_use_code", ""), r.get("property_type_label", ""),
            r.get("property_type_decision", ""), r.get("year_built"), r.get("building_count"),
        )
        if prop_id:
            _upsert_valuation_raw(
                conn, ts, prop_id, r.get("market_value"), r.get("assessed_value"),
                r.get("taxable_value"), r.get("last_sale_date"), r.get("last_sale_price"),
                r.get("years_owned"),
            )
        owner_id, owner_conf = _upsert_owner(
            conn, ts, r.get("owner", ""), r.get("mail_address", ""), r.get("mail_city", ""),
            r.get("mail_state", ""), r.get("mail_zip", ""), is_entity, is_trust, prop_id, doc_id,
        )

        if prop_id and owner_id:
            conn.execute(
                "INSERT INTO ownerships (property_id, owner_id, is_current, first_seen_at, last_seen_at) "
                "VALUES (?,?,1,?,?) ON CONFLICT(property_id, owner_id) DO UPDATE SET last_seen_at=excluded.last_seen_at",
                (prop_id, owner_id, ts, ts),
            )

        existing = conn.execute("SELECT id, first_seen_at FROM documents WHERE id=?", (doc_id,)).fetchone()
        first_seen = existing["first_seen_at"] if existing else ts
        conn.execute(
            "INSERT OR REPLACE INTO documents (id, doc_num, doc_type, cat, cat_label, filed_date, "
            "owner_name, other_party, legal_desc, amount, property_id, owner_id, code_violation_confidence, "
            "flags_json, legacy_score, source_name, source_url, scrape_run_id, raw_json, first_seen_at, last_seen_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (doc_id, doc_num, doc_type, r.get("cat", ""), r.get("cat_label", ""), r.get("filed", ""),
             r.get("owner", ""), r.get("grantee", ""), r.get("legal", ""), r.get("amount"),
             prop_id, owner_id, r.get("code_violation_confidence"),
             json.dumps(r.get("flags", [])), r.get("score"), source,
             r.get("clerk_url", ""), run_id, json.dumps(r, ensure_ascii=False), first_seen, ts),
        )

        for entity_type, entity_id, field_name, value, conf in (
            ("property", prop_id, "situs_address", r.get("prop_address", ""), prop_conf),
            ("owner", owner_id, "mailing_address", r.get("mail_address", ""), owner_conf),
        ):
            if not entity_id or not value:
                continue
            status = "VERIFIED" if conf in ("verified_parcel", "name_and_mailing_address") else "INFERRED"
            conn.execute(
                "INSERT INTO evidence (entity_type, entity_id, field_name, value, confidence_status, "
                "source_name, source_url, observed_at, is_current) VALUES (?,?,?,?,?,?,?,?,1)",
                (entity_type, entity_id, field_name, value, status, source, r.get("clerk_url", ""), ts),
            )

        if not r.get("prop_address"):
            # No property could be identified at all -- attach the task to
            # the document/owner we do have, not to a property_id (there
            # isn't one; that's exactly the gap being flagged).
            _add_research_task_if_absent(
                conn, "confirm_property_address", document_id=doc_id, owner_id=owner_id,
                description="Property Appraiser lookup failed to return a situs address for "
                             f"{r.get('owner', 'this owner')!r} -- confirm the property manually.",
                priority="normal",
            )

        if is_trust and owner_id:
            # A trust on title is a legitimate lead, not proof anyone we can
            # reach has authority to sell -- never assume a trustee,
            # beneficiary, relative, or occupant does.
            _add_research_task_if_absent(
                conn, "verify_trust_authority", property_id=prop_id, owner_id=owner_id,
                description=f"TRUST_OWNERSHIP_REVIEW_REQUIRED: confirm trustee identity and authority "
                             f"to sell for {owner_name!r} before treating any contact as decision-maker.",
                priority="normal",
            )

        if r.get("cat") == "probate" and prop_id:
            # DirectName on a probate record is conventionally the decedent --
            # a legitimate estate-sale lead, but not a person anyone can
            # contact. Same discipline as trust ownership: flag for review,
            # never assume a relative/heir/occupant has authority to sell.
            _add_research_task_if_absent(
                conn, "verify_probate_representative", property_id=prop_id, owner_id=owner_id,
                description=f"PROBATE_REPRESENTATIVE_REVIEW_REQUIRED: {owner_name!r} is recorded as the "
                             f"decedent -- identify the actual personal representative/heir with authority "
                             f"to sell before treating any contact as decision-maker.",
                priority="normal",
            )

        if r.get("property_type_decision") == "review" and prop_id:
            _add_research_task_if_absent(
                conn, "confirm_property_type", property_id=prop_id,
                description=f"PROPERTY_TYPE_REVIEW_REQUIRED: {r.get('property_type_label', 'ambiguous type')} "
                             f"(DOR code {r.get('property_use_code', '?')}) -- confirm this fits acquisition "
                             f"criteria before spending more time on it.",
                priority="normal",
            )

        n += 1
    conn.commit()
    return n


def _add_research_task_if_absent(conn, task_type, property_id=None, owner_id=None,
                                  document_id=None, description="", priority="normal"):
    key_col, key_val = ("property_id", property_id) if property_id else \
        ("document_id", document_id) if document_id else ("owner_id", owner_id)
    if not key_val:
        return
    existing = conn.execute(
        f"SELECT id FROM research_tasks WHERE {key_col}=? AND task_type=? AND status='open'",
        (key_val, task_type),
    ).fetchone()
    if existing:
        return
    conn.execute(
        "INSERT INTO research_tasks (property_id, owner_id, document_id, task_type, description, "
        "status, priority, created_at) VALUES (?,?,?,?,?,'open',?,?)",
        (property_id, owner_id, document_id, task_type, description, priority, now_iso()),
    )


# ---------------------------------------------------------------------------
# Phase 3: equity signal + Dealability Score.
#
# "The system must determine whether there is a plausible transaction
# before spending excessive time enriching the lead" -- so this runs from
# data already on hand (no new network calls): PAO's assessed/market
# value, ownership tenure, and the distress documents already scraped for
# this property. A real mortgage-balance lookup is a separate, deliberate
# per-property research task, not automatic enrichment for every lead --
# see mortgage_estimate, always 'UNKNOWN' here.
# ---------------------------------------------------------------------------

DEALABILITY_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "dealability_weights.json"
)


def load_dealability_config(path=DEALABILITY_CONFIG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


DEALABILITY_CONFIG = load_dealability_config()

# Categories that represent an active, unresolved distress claim against
# the property. "release" is the opposite (a prior lien being cleared) and
# "construction" (Notice of Commencement) isn't a claim at all, so neither
# counts toward the lien load.
ACTIVE_LIEN_CATS = {"lien", "judgment", "foreclosure", "code_violation", "tax", "probate"}


def compute_equity_signal(years_owned, last_sale_price, market_value, config=None):
    """Returns (signal, confidence, reasoning, appreciated). Deliberately a
    category + plain-language reason, not a fabricated dollar figure -- no
    mortgage data exists to net against a value, so a dollar 'equity'
    number here would just be the value estimate wearing a different name.
    `appreciated` is exposed so callers scoring the bonus don't have to
    re-derive the same (nominal-sale-price-guarded) comparison themselves."""
    config = config if config is not None else DEALABILITY_CONFIG
    eq = config.get("equity_signal", {})

    if years_owned is None:
        return "UNKNOWN", "UNKNOWN", "No sale history found -- ownership tenure unknown.", False

    strong_ratio = eq.get("appreciation_ratio_strong", 1.3)
    # A recorded sale price under this floor is essentially always nominal
    # consideration -- a family transfer, a quit-claim, a $1/$10/$100 deed
    # -- not a real arm's-length sale. Dividing market value by a nominal
    # price produces a nonsense multiplier (seen live: "risen 3671x" off a
    # $100 sale), so those prices don't feed the appreciation claim at all;
    # years_owned alone still drives the tenure signal either way.
    nominal_floor = eq.get("nominal_sale_price_floor", 1000)
    real_sale_price = last_sale_price if (last_sale_price and last_sale_price >= nominal_floor) else None
    appreciated = (
        real_sale_price and market_value
        and (market_value / real_sale_price) >= strong_ratio
    )

    if years_owned >= eq.get("years_owned_substantial", 10):
        reason = f"Owned {years_owned} years -- long enough that meaningful principal paydown and/or appreciation is plausible, even with no mortgage data."
        if appreciated:
            reason += f" County value has also risen {market_value/real_sale_price:.1f}x since the last recorded (non-nominal) sale."
        return "LIKELY_SUBSTANTIAL", "ESTIMATED", reason, bool(appreciated)

    if years_owned >= eq.get("years_owned_moderate", 4):
        reason = f"Owned {years_owned} years -- some paydown/appreciation is plausible, but not long enough to assume it confidently."
        return "LIKELY_MODERATE", "ESTIMATED", reason, bool(appreciated)

    reason = f"Purchased only {years_owned} year{'s' if years_owned != 1 else ''} ago -- a recent purchase is more likely to carry a large mortgage relative to value; treat equity as unproven, not absent."
    return "LIKELY_LIMITED", "INFERRED", reason, bool(appreciated)


# ---------------------------------------------------------------------------
# Phase 4: tax-deed timeline.
#
# Built entirely from the one verified date this pipeline actually
# scrapes -- the scheduled tax deed auction date from
# duval.realtaxdeed.com (documents.filed_date for doc_type='TAX DEED').
# Florida's real tax process has earlier stages (delinquency, certificate
# sale, the 2-year certificate-holder eligibility window) that this
# pipeline does not scrape a source for, so they are never claimed here --
# a property with no scheduled auction on file is TAX_STAGE_NONE, not
# assumed current on its taxes.
# ---------------------------------------------------------------------------

TAX_TIMELINE_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "tax_timeline.json"
)


def load_tax_timeline_config(path=TAX_TIMELINE_CONFIG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


TAX_TIMELINE_CONFIG = load_tax_timeline_config()


def _parse_taxdeed_date(s):
    """Parses the scraper's MM-DD-YYYY auction date string (guaranteed by
    the \\d{2}/\\d{2}/\\d{4} regex that produced it upstream, not
    free-form scraped text). Returns a date or None."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%m-%d-%Y").date()
    except (ValueError, TypeError):
        return None


def compute_tax_deed_timeline(conn, property_id, config=None, today=None):
    """Returns (stage, days_until_sale, runway_points, reasoning) for one
    property. `days_until_sale` is None when no scheduled auction is on
    file (TAX_STAGE_NONE) or its date didn't parse, negative once the
    scheduled date has passed (outcome not tracked by this pipeline --
    flagged for manual verification, not assumed sold)."""
    config = config if config is not None else TAX_TIMELINE_CONFIG
    today = today or datetime.date.today()
    rp = config.get("runway_points", {})

    row = conn.execute(
        "SELECT filed_date FROM documents WHERE property_id=? AND doc_type='TAX DEED' "
        "ORDER BY last_seen_at DESC LIMIT 1",
        (property_id,),
    ).fetchone()
    if not row or not row["filed_date"]:
        return "TAX_STAGE_NONE", None, rp.get("none", 15), (
            "No scheduled tax deed auction on file for this property."
        )

    auction_date = _parse_taxdeed_date(row["filed_date"])
    if auction_date is None:
        return "TAX_STAGE_NONE", None, rp.get("none", 15), (
            f"Tax deed record on file but its auction date ({row['filed_date']!r}) didn't parse -- "
            "treating as unknown rather than guessing."
        )

    days = (auction_date - today).days
    if days < 0:
        return "TAX_STAGE_PAST_UNVERIFIED", days, rp.get("past_unverified", 0), (
            f"Scheduled auction date ({auction_date.isoformat()}) has passed -- outcome (sold, redeemed, "
            "postponed) isn't tracked by this pipeline; verify manually before treating this as a live deal."
        )
    if days <= config.get("imminent_days", 30):
        return "TAX_STAGE_IMMINENT", days, rp.get("imminent", 4), (
            f"Tax deed auction in {days} day{'s' if days != 1 else ''} ({auction_date.isoformat()}) -- "
            "very little time to close before the county sells it; verify closing feasibility now."
        )
    if days <= config.get("soon_days", 90):
        return "TAX_STAGE_SOON", days, rp.get("soon", 12), (
            f"Tax deed auction in {days} days ({auction_date.isoformat()}) -- workable but a tightening window to close."
        )
    return "TAX_STAGE_FAR", days, rp.get("far", 20), (
        f"Tax deed auction in {days} days ({auction_date.isoformat()}) -- ample time to close before the county sells it."
    )


# ---------------------------------------------------------------------------
# Phase 5: foreclosure timeline.
#
# Unlike Phase 4's tax-deed timeline, there is no scraped foreclosure-sale-
# calendar source with an exact date -- Duval's Official Records site
# gives filing dates, not auction dates. So this is built from two real,
# verified judicial milestones already in `documents`: the Lis Pendens
# filing date (cat='foreclosure') and, once one exists, the Final
# Judgment date (doc_type in RPO/VA FINAL JUDGMENT, cat='judgment'). No
# exact days-until-sale is ever claimed here -- only real elapsed time
# since a real recorded milestone, with Florida Statute 45.031's typical
# 20-35-day post-judgment sale window cited as context, not fabricated
# as a scraped fact.
# ---------------------------------------------------------------------------

FORECLOSURE_TIMELINE_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "foreclosure_timeline.json"
)


def load_foreclosure_timeline_config(path=FORECLOSURE_TIMELINE_CONFIG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


FORECLOSURE_TIMELINE_CONFIG = load_foreclosure_timeline_config()

# The two Official Records doc types that are specifically a foreclosure
# FINAL judgment (as opposed to a generic money JUDGMENT, which is far
# more common and not evidence a sale is anywhere close) -- see
# CATEGORY_MAP in the scraper, which stores doc_type as this exact
# uppercase text regardless of the county API's own casing.
FORECLOSURE_FINAL_JUDGMENT_TYPES = ("RPO FINAL JUDGMENT", "VA FINAL JUDGMENT")


def _parse_iso_date(s):
    """Parses Official Records' YYYY-MM-DD filed_date (confirmed against
    real scraped data, as opposed to tax deed's MM-DD-YYYY). Returns a
    date or None."""
    if not s:
        return None
    try:
        return datetime.date.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def compute_foreclosure_timeline(conn, property_id, config=None, today=None):
    """Returns (stage, days, runway_points, reasoning) for one property.
    `days` is days since the milestone the stage is keyed on (judgment
    entry if one exists, else the original Lis Pendens filing) -- never a
    countdown to a sale date this pipeline doesn't have."""
    config = config if config is not None else FORECLOSURE_TIMELINE_CONFIG
    today = today or datetime.date.today()
    rp = config.get("runway_points", {})

    judgment_row = conn.execute(
        "SELECT filed_date FROM documents WHERE property_id=? AND cat='judgment' AND doc_type IN "
        f"({','.join('?' for _ in FORECLOSURE_FINAL_JUDGMENT_TYPES)}) "
        "ORDER BY filed_date DESC LIMIT 1",
        (property_id, *FORECLOSURE_FINAL_JUDGMENT_TYPES),
    ).fetchone()

    if judgment_row and judgment_row["filed_date"]:
        judgment_date = _parse_iso_date(judgment_row["filed_date"])
        if judgment_date is not None:
            days_since = (today - judgment_date).days
            recent_cutoff = config.get("judgment_recent_days", 45)
            if days_since <= recent_cutoff:
                when = f"{days_since} day{'s' if days_since != 1 else ''} ago" if days_since >= 0 else "recently"
                return "FORECLOSURE_STAGE_JUDGMENT_ENTERED", days_since, rp.get("judgment_entered", 4), (
                    f"Final judgment entered {when} ({judgment_date.isoformat()}) -- Florida law (Stat. 45.031) "
                    "typically schedules the sale 20-35 days after judgment, so a sale is likely imminent even "
                    "though this pipeline doesn't scrape the exact sale date; verify it before counting on it."
                )
            return "FORECLOSURE_STAGE_JUDGMENT_STALE", days_since, rp.get("judgment_stale", 0), (
                f"Final judgment was entered {days_since} days ago ({judgment_date.isoformat()}) -- well past the "
                "typical 20-35 day statutory sale window, so the outcome (sold, postponed, redeemed, dismissed) "
                "isn't tracked by this pipeline; verify manually before treating this as a live deal."
            )

    lp_row = conn.execute(
        "SELECT filed_date FROM documents WHERE property_id=? AND cat='foreclosure' "
        "ORDER BY filed_date ASC LIMIT 1",
        (property_id,),
    ).fetchone()
    if not lp_row or not lp_row["filed_date"]:
        return "FORECLOSURE_STAGE_NONE", None, rp.get("none", 15), (
            "No Lis Pendens on file for this property."
        )
    filed_date = _parse_iso_date(lp_row["filed_date"])
    if filed_date is None:
        return "FORECLOSURE_STAGE_NONE", None, rp.get("none", 15), (
            f"Lis Pendens record on file but its filed date ({lp_row['filed_date']!r}) didn't parse -- "
            "treating as unknown rather than guessing."
        )
    days_since_filed = (today - filed_date).days
    return "FORECLOSURE_STAGE_FILED", days_since_filed, rp.get("filed", 12), (
        f"Lis Pendens filed {days_since_filed} day{'s' if days_since_filed != 1 else ''} ago "
        f"({filed_date.isoformat()}) -- no final judgment on file yet; foreclosure case duration varies "
        "widely (months to years), so this is informational, not a countdown."
    )


# ---------------------------------------------------------------------------
# Phase 6: code enforcement signal.
#
# Duval's Official Records has no distinct "Code Violation" doc type --
# these are already an inferred lien-proxy (a generic LIEN filed by a
# municipal entity, see MUNICIPAL_FILER_PATTERN in the scraper), and this
# builds on that same "clearly labeled inference, never a confirmed case"
# discipline: no fine amount, case status, or hearing date is scraped, so
# this is built purely from real, countable facts already in `documents`
# -- how many such liens exist for a property, and how recent the latest
# one is. Deliberately NOT wired into the Dealability Score (which is
# about deal feasibility, not seller motivation/distress) -- this is
# seller-distress signal data for the stacked-distress/contact-priority
# engine, Phase 8.
# ---------------------------------------------------------------------------

CODE_ENFORCEMENT_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "code_enforcement.json"
)


def load_code_enforcement_config(path=CODE_ENFORCEMENT_CONFIG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


CODE_ENFORCEMENT_CONFIG = load_code_enforcement_config()


def compute_code_enforcement_signal(conn, property_id, config=None, today=None):
    """Returns (stage, count, days_since_most_recent, reasoning) for one
    property, built entirely from documents already recorded with
    cat='code_violation'. `count` >= 2 (CODE_STAGE_REPEAT) is the
    strongest real signal here -- a property with more than one municipal
    lien recorded against it, a genuine chronic-neglect pattern, not an
    inference stacked on an inference."""
    config = config if config is not None else CODE_ENFORCEMENT_CONFIG
    today = today or datetime.date.today()

    rows = conn.execute(
        "SELECT filed_date FROM documents WHERE property_id=? AND cat='code_violation' "
        "ORDER BY filed_date DESC",
        (property_id,),
    ).fetchall()
    count = len(rows)

    if count == 0:
        return "CODE_STAGE_NONE", 0, None, "No code violation lien on file for this property."

    most_recent = _parse_iso_date(rows[0]["filed_date"]) if rows[0]["filed_date"] else None
    days_since = (today - most_recent).days if most_recent else None

    if count >= 2:
        return "CODE_STAGE_REPEAT", count, days_since, (
            f"{count} separate code violation liens recorded against this property -- a chronic pattern, "
            "not an isolated incident, whether or not any were later resolved."
        )

    recent_cutoff = config.get("recent_days", 180)
    if days_since is not None and days_since <= recent_cutoff:
        return "CODE_STAGE_SINGLE_RECENT", count, days_since, (
            f"One code violation lien recorded {days_since} day{'s' if days_since != 1 else ''} ago -- "
            "a single, recent filing."
        )
    if days_since is not None:
        return "CODE_STAGE_SINGLE_AGED", count, days_since, (
            f"One code violation lien recorded {days_since} days ago -- an older, isolated filing. This "
            "pipeline can't confirm whether it was ever resolved (no reliable cross-reference from a "
            "satisfaction/release record back to the lien it clears)."
        )
    return "CODE_STAGE_SINGLE_RECENT", count, None, (
        "One code violation lien on file but its filed date didn't parse -- treating recency as unknown."
    )


# ---------------------------------------------------------------------------
# Phase 7: seller-profile signals -- absentee ownership and landlord
# portfolio/fatigue. Built entirely from address and ownership data
# already scraped and persisted (no new source, no network call). Like
# Phase 6, deliberately NOT wired into the Dealability Score -- these are
# seller-distress/motivation signals for the stacked-distress/contact-
# priority engine (a later phase), not deal-feasibility ones.
#
# Eviction filings and vacancy status are explicitly NOT built here:
# Duval's Official Records has no eviction/landlord-tenant case data
# (that lives in a separate court case management system this pipeline
# doesn't scrape) and there's no free, reliable vacancy-verification
# source -- fabricating either would violate the same "never claim data
# we don't have" discipline as every prior phase.
# ---------------------------------------------------------------------------

SELLER_PROFILE_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "seller_profile.json"
)


def load_seller_profile_config(path=SELLER_PROFILE_CONFIG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


SELLER_PROFILE_CONFIG = load_seller_profile_config()


def compute_absentee_signal(conn, property_id):
    """Returns (stage, reasoning). Compares each current owner's mailing
    address against the property's own situs address (reusing _norm(),
    the same normalization used for identity hashing, so trivial
    formatting differences don't produce a false absentee flag). Missing
    address data is UNKNOWN, never silently treated as owner-occupied."""
    prop = conn.execute(
        "SELECT situs_address FROM properties WHERE id=?", (property_id,)
    ).fetchone()
    owners = conn.execute(
        "SELECT o.mailing_address, o.mailing_state FROM ownerships ow "
        "JOIN owners o ON o.id = ow.owner_id WHERE ow.property_id=?",
        (property_id,),
    ).fetchall()

    if not prop or not prop["situs_address"] or not owners:
        return "ABSENTEE_STAGE_UNKNOWN", "Insufficient address data to compare owner mailing address against the property address."

    situs_norm = _norm(prop["situs_address"])
    any_matched = False
    any_out_of_state = False
    any_local_absentee = False
    for o in owners:
        mail_addr = o["mailing_address"] or ""
        if not mail_addr:
            continue
        if _norm(mail_addr) == situs_norm:
            any_matched = True
            continue
        if (o["mailing_state"] or "").strip().upper() not in ("FL", ""):
            any_out_of_state = True
        else:
            any_local_absentee = True

    if not any_matched and not any_out_of_state and not any_local_absentee:
        return "ABSENTEE_STAGE_UNKNOWN", "No owner mailing address on file to compare against the property address."
    if any_out_of_state:
        return "ABSENTEE_STAGE_OUT_OF_STATE", "Owner's mailing address is outside Florida -- the strongest absentee signal (a distant property is harder to manage or maintain)."
    if any_local_absentee:
        return "ABSENTEE_STAGE_LOCAL_ABSENTEE", "Owner's mailing address differs from the property address but is still in Florida -- owns it without living there (rental, inherited, or second property)."
    return "ABSENTEE_STAGE_OWNER_OCCUPIED_LIKELY", "Owner's mailing address matches the property address -- likely owner-occupied."


def compute_landlord_portfolio_signal(conn, property_id, config=None):
    """Returns (stage, portfolio_property_count, portfolio_distressed_count,
    reasoning). Counts DISTINCT properties linked to the same owner(s) as
    this property, in this pipeline's own (deliberately conservative)
    owner identity matching -- a floor on real portfolio size, not a
    ceiling, since under-merging owner identity is intentional (see the
    module docstring)."""
    config = config if config is not None else SELLER_PROFILE_CONFIG
    threshold = config.get("landlord_multi_property_threshold", 2)

    owner_ids = [r["owner_id"] for r in conn.execute(
        "SELECT DISTINCT owner_id FROM ownerships WHERE property_id=?", (property_id,)
    )]
    if not owner_ids:
        return "LANDLORD_STAGE_UNKNOWN", 0, 0, "No confirmed owner on file to check for other properties."

    placeholders = ",".join("?" for _ in owner_ids)
    portfolio_props = {r["property_id"] for r in conn.execute(
        f"SELECT DISTINCT property_id FROM ownerships WHERE owner_id IN ({placeholders})", owner_ids
    )}
    portfolio_count = len(portfolio_props)

    if portfolio_count < threshold:
        return "LANDLORD_STAGE_SINGLE", portfolio_count, 0, "Only one property on file for this owner -- no portfolio pattern detected."

    prop_placeholders = ",".join("?" for _ in portfolio_props)
    cat_placeholders = ",".join("?" for _ in ACTIVE_LIEN_CATS)
    distressed_props = {r["property_id"] for r in conn.execute(
        f"SELECT DISTINCT property_id FROM documents WHERE property_id IN ({prop_placeholders}) "
        f"AND cat IN ({cat_placeholders})",
        (*portfolio_props, *ACTIVE_LIEN_CATS),
    )}
    distressed_count = len(distressed_props)

    if distressed_count >= threshold:
        return "LANDLORD_STAGE_MULTI_PROPERTY_DISTRESS", portfolio_count, distressed_count, (
            f"{distressed_count} of this owner's {portfolio_count} known properties carry active distress "
            "documents -- a portfolio owner facing trouble on multiple holdings at once, not an isolated case."
        )
    return "LANDLORD_STAGE_MULTI_PROPERTY", portfolio_count, distressed_count, (
        f"This owner has {portfolio_count} properties on file in this pipeline's data, but only this one "
        "currently carries an active distress document."
    )


def compute_dealability_score(conn, property_id, config=None):
    """Computes the Dealability Score (0-100) for one property from
    what's already in the database -- no network calls. Returns
    (score, reason, equity_signal, equity_confidence, equity_reasoning,
    active_lien_count, has_tax_distress, tax_deed_stage, days_until_tax_sale,
    foreclosure_stage, days_since_foreclosure_milestone)."""
    config = config if config is not None else DEALABILITY_CONFIG
    mp = config.get("max_points", {})
    reasons = []

    prop = conn.execute(
        "SELECT property_type_decision FROM properties WHERE id=?", (property_id,)
    ).fetchone()
    val = conn.execute(
        "SELECT years_owned, last_sale_price, pao_market_value, pao_assessed_value "
        "FROM valuations WHERE property_id=?", (property_id,)
    ).fetchone()

    years_owned = val["years_owned"] if val else None
    last_sale_price = val["last_sale_price"] if val else None
    market_value = val["pao_market_value"] if val else None

    # -- Equity signal --
    signal, eq_conf, eq_reason, appreciated = compute_equity_signal(years_owned, last_sale_price, market_value, config)
    eq_cfg = config.get("equity_signal", {})
    equity_points = {
        "LIKELY_SUBSTANTIAL": eq_cfg.get("years_owned_substantial_points", 30),
        "LIKELY_MODERATE": eq_cfg.get("years_owned_moderate_points", 18),
        "LIKELY_LIMITED": eq_cfg.get("years_owned_recent_points", 8),
        "UNKNOWN": eq_cfg.get("unknown_tenure_points", 10),
    }.get(signal, 0)
    if signal == "LIKELY_SUBSTANTIAL" and appreciated:
        equity_points = min(equity_points + eq_cfg.get("appreciation_bonus_points", 5), mp.get("equity_signal", 35))
    equity_points = min(equity_points, mp.get("equity_signal", 35))
    reasons.append(f"Equity: {signal.replace('_',' ').title()} ({equity_points}/{mp.get('equity_signal',35)}) -- {eq_reason}")

    # -- Manageable liens: total count of active distress documents
    # recorded against this property, from what we've already scraped --
    # real, verified counts, not dollar amounts (which aren't captured).
    # This is property-level (a property can have multiple documents/leads
    # feeding it -- that's what Compound Distress tracks), so it includes
    # whichever document brought this property in: every lead has at least
    # 1 by definition, which is why the "clear" threshold below is 1, not
    # 0 -- it means "nothing beyond the single signal that flagged it."
    lien_row = conn.execute(
        "SELECT COUNT(*) c FROM documents WHERE property_id=? AND cat IN "
        f"({','.join('?' for _ in ACTIVE_LIEN_CATS)})",
        (property_id, *ACTIVE_LIEN_CATS),
    ).fetchone()
    active_lien_count = lien_row["c"] if lien_row else 0
    tax_row = conn.execute(
        "SELECT COUNT(*) c FROM documents WHERE property_id=? AND cat='tax'", (property_id,)
    ).fetchone()
    has_tax_distress = bool(tax_row and tax_row["c"])

    lt = config.get("lien_thresholds", {})
    if active_lien_count <= lt.get("clear_max", 0):
        lien_points = lt.get("clear_points", 10)
        lien_desc = "no other active distress recorded against this property"
    elif active_lien_count <= lt.get("manageable_max", 2):
        lien_points = lt.get("manageable_points", 5)
        lien_desc = f"{active_lien_count} active distress records on this property"
    else:
        lien_points = lt.get("heavy_points", 0)
        lien_desc = f"{active_lien_count} active distress records -- heavily encumbered"
    reasons.append(f"Liens: {lien_points}/{mp.get('manageable_liens',10)} -- {lien_desc} (mortgage balance unknown -- see research queue).")

    # -- Ownership clarity: from Phase 1's identity confidence + trust/entity flags --
    owners = conn.execute(
        "SELECT o.is_entity, o.is_trust, o.identity_confidence FROM ownerships ow "
        "JOIN owners o ON o.id = ow.owner_id WHERE ow.property_id=?", (property_id,)
    ).fetchall()
    oc = config.get("ownership_clarity", {})
    if not owners:
        owner_points = oc.get("unconfirmed_identity_points", 4)
        owner_desc = "no confirmed owner on file"
    else:
        any_trust = any(o["is_trust"] for o in owners)
        any_entity = any(o["is_entity"] for o in owners)
        best_conf = any(o["identity_confidence"] in ("verified_parcel", "name_and_mailing_address") for o in owners)
        if any_trust:
            owner_points = oc.get("trust_review_required_points", 6)
            owner_desc = "trust ownership -- trustee authority not yet verified"
        elif any_entity:
            owner_points = oc.get("entity_owned_points", 8)
            owner_desc = "entity-held title"
        elif best_conf:
            owner_points = oc.get("verified_individual_points", 15)
            owner_desc = "individual owner, identity confirmed"
        else:
            owner_points = oc.get("unconfirmed_identity_points", 4)
            owner_desc = "owner identity not yet confirmed"
        if len(owners) > 1:
            owner_points = max(0, owner_points - oc.get("multiple_owners_penalty", 3))
            owner_desc += f"; {len(owners)} owners on file, all must be accounted for"
    reasons.append(f"Ownership clarity: {owner_points}/{mp.get('ownership_clarity',15)} -- {owner_desc}.")

    # -- Fits acquisition criteria: Phase 2's classification --
    decision = prop["property_type_decision"] if prop else None
    fits_points = mp.get("fits_acquisition_criteria", 10) if decision == "include" else \
        (mp.get("fits_acquisition_criteria", 10) // 2 if decision == "review" else 0)
    reasons.append(f"Acquisition criteria fit: {fits_points}/{mp.get('fits_acquisition_criteria',10)} -- property type is '{decision or 'unknown'}'.")

    # -- Reliable value estimate: did PAO enrichment succeed at all --
    vr = config.get("value_reliability", {})
    value_points = vr.get("matched_points", 10) if market_value else vr.get("unmatched_points", 0)
    reasons.append(f"Value reliability: {value_points}/{mp.get('reliable_value_estimate',10)} -- "
                    f"{'PAO market/assessed value on file' if market_value else 'no PAO value found for this property'}.")

    # -- Closing runway: real timeline data when it exists, tax-deed first
    # (Phase 4 -- an exact scraped auction date) then foreclosure (Phase 5
    # -- real milestones, no exact date), else a neutral placeholder. A
    # property could in theory carry both a tax-deed and a foreclosure
    # signal; tax-deed wins because it's the more precise (exact date)
    # signal of the two.
    tax_stage, days_until_tax_sale, tax_runway_points, tax_runway_reason = compute_tax_deed_timeline(conn, property_id)
    foreclosure_stage, days_since_foreclosure_milestone, fc_runway_points, fc_runway_reason = \
        compute_foreclosure_timeline(conn, property_id)
    if tax_stage != "TAX_STAGE_NONE":
        runway_points, runway_reason = tax_runway_points, tax_runway_reason
    elif foreclosure_stage != "FORECLOSURE_STAGE_NONE":
        runway_points, runway_reason = fc_runway_points, fc_runway_reason
    else:
        runway_points, runway_reason = tax_runway_points, (
            "No scheduled tax deed auction or Lis Pendens on file for this property -- neutral placeholder."
        )
    runway_points = min(runway_points, mp.get("closing_runway", 20))
    reasons.append(f"Closing runway: {runway_points}/{mp.get('closing_runway',20)} -- {runway_reason}")

    # A hard cap for confirmed negative equity is intentionally not wired
    # in yet: no data source here actually confirms negative equity (that
    # needs a real mortgage payoff, Tier 2, not yet built) -- applying
    # config's negative_equity_cap against an unconfirmed signal would be
    # penalizing a guess as if it were a fact. Revisit once Tier 2 lands.
    total = max(0, min(100, equity_points + lien_points + owner_points + fits_points + value_points + runway_points))
    reason = " | ".join(reasons)
    return (total, reason, signal, eq_conf, eq_reason, active_lien_count, has_tax_distress,
            tax_stage, days_until_tax_sale, foreclosure_stage, days_since_foreclosure_milestone)


def compute_dealability_for_all_properties(conn, run_id, config=None):
    """Runs after persist_records() for the run. Re-scores every property
    on file, not just ones touched this run -- cheap (pure SQL, no
    network) and keeps every score consistent with the latest data.
    Appends one scores row per property (the audit log) and refreshes
    that property's valuations row. Properties that clear a minimum
    dealability bar get a Tier 2 'estimate_mortgage_payoff' research task
    queued -- the real payoff lookup stays a deliberate per-property
    action, never automatic enrichment for every lead."""
    config = config if config is not None else DEALABILITY_CONFIG
    ts = now_iso()
    tier2_threshold = config.get("tier2_mortgage_lookup_threshold", 50)
    property_ids = [r["id"] for r in conn.execute("SELECT id FROM properties")]
    for property_id in property_ids:
        (score, reason, signal, eq_conf, eq_reason, lien_count, has_tax,
         tax_stage, days_until_tax_sale, foreclosure_stage, days_since_foreclosure_milestone) = \
            compute_dealability_score(conn, property_id, config)
        code_stage, code_count, _code_days, _code_reason = compute_code_enforcement_signal(conn, property_id)
        absentee_stage, _absentee_reason = compute_absentee_signal(conn, property_id)
        landlord_stage, portfolio_count, portfolio_distressed_count, _landlord_reason = \
            compute_landlord_portfolio_signal(conn, property_id)
        conn.execute(
            "UPDATE valuations SET active_lien_count=?, has_tax_distress=?, "
            "equity_signal=?, equity_confidence=?, equity_reasoning=?, "
            "tax_deed_stage=?, days_until_tax_sale=?, "
            "foreclosure_stage=?, days_since_foreclosure_milestone=?, "
            "code_enforcement_stage=?, code_violation_count=?, "
            "absentee_stage=?, landlord_portfolio_stage=?, "
            "portfolio_property_count=?, portfolio_distressed_count=?, "
            "value_confidence=?, computed_at=? WHERE property_id=?",
            (lien_count, int(has_tax), signal, eq_conf, eq_reason,
             tax_stage, days_until_tax_sale,
             foreclosure_stage, days_since_foreclosure_milestone,
             code_stage, code_count,
             absentee_stage, landlord_stage, portfolio_count, portfolio_distressed_count,
             "ESTIMATED" if signal != "UNKNOWN" else "UNKNOWN", ts, property_id),
        )
        conn.execute(
            "INSERT INTO scores (property_id, computed_at, dealability_score, dealability_reason, "
            "weights_version, scrape_run_id) VALUES (?,?,?,?,?,?)",
            (property_id, ts, score, reason, "phase3-v1", run_id),
        )
        if score >= tier2_threshold:
            _add_research_task_if_absent(
                conn, "estimate_mortgage_payoff", property_id=property_id,
                description=f"Dealability Score {score}/100 -- worth a real mortgage-payoff lookup "
                             f"(Official Records name/parcel history search) before assuming the equity signal.",
                priority="normal",
            )
    conn.commit()
    return len(property_ids)


# ---------------------------------------------------------------------------
# Merging in dashboard-originated state (suppression/notes/overrides/
# approvals) synced from the published artifact's database.
# ---------------------------------------------------------------------------

def merge_dashboard_state(conn, suppressions=None, contacts=None, overrides=None, hubspot_approvals=None):
    """Each argument is a list of dicts shaped like the corresponding
    table's columns (minus autoincrement id). Idempotent-ish: this is
    additive log data, so duplicates from re-syncing the same artifact
    read are acceptable and harmless for suppressions/contacts/overrides;
    hubspot_export_flags is upserted by (entity_type, entity_id)."""
    n = 0
    for s in suppressions or []:
        conn.execute(
            "INSERT INTO suppressions (entity_type, entity_id, reason_code, note, created_by, created_at, expires_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (s["entity_type"], s["entity_id"], s["reason_code"], s.get("note", ""),
             s.get("created_by", ""), s.get("created_at", now_iso()), s.get("expires_at")),
        )
        n += 1
    for c in contacts or []:
        conn.execute(
            "INSERT INTO contacts (owner_id, property_id, channel, occurred_at, outcome, note, created_by, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (c.get("owner_id"), c.get("property_id"), c.get("channel", ""), c.get("occurred_at", now_iso()),
             c.get("outcome", ""), c.get("note", ""), c.get("created_by", ""), c.get("created_at", now_iso())),
        )
        n += 1
    for o in overrides or []:
        conn.execute(
            "INSERT INTO overrides (entity_type, entity_id, field_name, old_value, new_value, created_by, reason, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (o["entity_type"], o["entity_id"], o["field_name"], o.get("old_value"), o.get("new_value"),
             o.get("created_by", ""), o.get("reason", ""), o.get("created_at", now_iso())),
        )
        n += 1
    for h in hubspot_approvals or []:
        conn.execute(
            "INSERT INTO hubspot_export_flags (entity_type, entity_id, approved_by, approved_at, exported_at, hubspot_record_id) "
            "VALUES (?,?,?,?,?,?) ON CONFLICT(entity_type, entity_id) DO UPDATE SET "
            "approved_by=excluded.approved_by, approved_at=excluded.approved_at",
            (h["entity_type"], h["entity_id"], h.get("approved_by", ""), h.get("approved_at", now_iso()),
             h.get("exported_at"), h.get("hubspot_record_id")),
        )
        n += 1
    conn.commit()
    return n


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

def _dump_table(conn, table):
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    return [dict(r) for r in rows]


def export_backup_json(conn, path):
    """The real backup: every table, in full, committed to git on every
    run. This -- not the SQLite file, not the artifact -- is the durable
    system of record."""
    data = {"exported_at": now_iso()}
    for table in TABLES:
        data[table] = _dump_table(conn, table)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=None, ensure_ascii=False)
    return data


def _split_name(full):
    full = (full or "").strip()
    if not full:
        return "", ""
    if "," in full:
        last, _, rest = full.partition(",")
        return rest.strip(), last.strip()
    parts = full.split()
    if len(parts) == 1:
        return "", parts[0]
    return " ".join(parts[:-1]), parts[-1]


def export_csv(conn, path):
    """Full non-suppressed lead export, independent of the artifact --
    this is what makes the data portable even if the dashboard is
    unreachable."""
    suppressed_props = {
        r["entity_id"] for r in conn.execute(
            "SELECT DISTINCT entity_id FROM suppressions WHERE entity_type='property' "
            "AND (expires_at IS NULL OR expires_at > ?)", (now_iso(),)
        )
    }
    rows = conn.execute(
        "SELECT d.*, p.re_number, p.situs_address, p.situs_city, p.situs_state, p.situs_zip, "
        "p.address_match_confidence, p.first_seen_at AS property_first_seen, "
        "p.property_type_label, p.property_type_decision, p.year_built, "
        "o.display_name, o.mailing_address, o.mailing_city, o.mailing_state, o.mailing_zip, "
        "o.identity_confidence, "
        "v.pao_assessed_value, v.pao_market_value, v.equity_signal, v.equity_confidence, "
        "v.active_lien_count, v.mortgage_estimate, v.tax_deed_stage, v.days_until_tax_sale, "
        "v.foreclosure_stage, v.days_since_foreclosure_milestone, "
        "v.code_enforcement_stage, v.code_violation_count, "
        "v.absentee_stage, v.landlord_portfolio_stage, v.portfolio_property_count, v.portfolio_distressed_count, "
        "(SELECT dealability_score FROM scores s WHERE s.property_id = p.id "
        " ORDER BY computed_at DESC LIMIT 1) AS dealability_score "
        "FROM documents d "
        "LEFT JOIN properties p ON p.id = d.property_id "
        "LEFT JOIN owners o ON o.id = d.owner_id "
        "LEFT JOIN valuations v ON v.property_id = d.property_id "
        "ORDER BY d.filed_date DESC"
    ).fetchall()

    cols = ["property_id", "owner_id", "First Name", "Last Name", "Owner Display Name",
            "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip", "Owner Identity Confidence",
            "Property RE#", "Property Address", "Property City", "Property State", "Property Zip",
            "Address Match Confidence", "Property Type", "Property Type Decision", "Year Built",
            "PAO Assessed Value", "PAO Market Value", "Equity Signal", "Equity Confidence",
            "Active Lien Count", "Mortgage Estimate", "Tax Deed Stage", "Days Until Tax Sale",
            "Foreclosure Stage", "Days Since Foreclosure Milestone",
            "Code Enforcement Stage", "Code Violation Count", "Code Violation Confidence (this document)",
            "Absentee Owner Stage", "Landlord Portfolio Stage", "Portfolio Property Count", "Portfolio Distressed Count",
            "Dealability Score",
            "Suppressed", "Category", "Document Type", "Filed Date",
            "Document Number", "Amount", "Legal Description", "Flags", "Legacy Score", "Source",
            "First Seen", "Last Seen"]

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            first, last = _split_name(r["display_name"])
            w.writerow([
                r["property_id"] or "", r["owner_id"] or "", first, last, r["display_name"] or "",
                r["mailing_address"] or "", r["mailing_city"] or "", r["mailing_state"] or "",
                r["mailing_zip"] or "", r["identity_confidence"] or "",
                r["re_number"] or "", r["situs_address"] or "", r["situs_city"] or "",
                r["situs_state"] or "", r["situs_zip"] or "", r["address_match_confidence"] or "",
                r["property_type_label"] or "", r["property_type_decision"] or "", r["year_built"] or "",
                r["pao_assessed_value"] or "", r["pao_market_value"] or "",
                r["equity_signal"] or "", r["equity_confidence"] or "",
                r["active_lien_count"] if r["active_lien_count"] is not None else "",
                r["mortgage_estimate"] or "UNKNOWN",
                r["tax_deed_stage"] or "TAX_STAGE_NONE",
                r["days_until_tax_sale"] if r["days_until_tax_sale"] is not None else "",
                r["foreclosure_stage"] or "FORECLOSURE_STAGE_NONE",
                r["days_since_foreclosure_milestone"] if r["days_since_foreclosure_milestone"] is not None else "",
                r["code_enforcement_stage"] or "CODE_STAGE_NONE",
                r["code_violation_count"] if r["code_violation_count"] is not None else "",
                r["code_violation_confidence"] or "",
                r["absentee_stage"] or "ABSENTEE_STAGE_UNKNOWN",
                r["landlord_portfolio_stage"] or "LANDLORD_STAGE_UNKNOWN",
                r["portfolio_property_count"] if r["portfolio_property_count"] is not None else "",
                r["portfolio_distressed_count"] if r["portfolio_distressed_count"] is not None else "",
                r["dealability_score"] if r["dealability_score"] is not None else "",
                "yes" if r["property_id"] in suppressed_props else "no",
                r["cat"] or "", r["doc_type"] or "", r["filed_date"] or "", r["doc_num"] or "",
                r["amount"] or "", r["legal_desc"] or "",
                "; ".join(json.loads(r["flags_json"] or "[]")), r["legacy_score"],
                r["source_name"] or "", r["first_seen_at"] or "", r["last_seen_at"] or "",
            ])
    return len(rows)


def export_hubspot_csv(conn, path):
    """Only rows explicitly approved via hubspot_export_flags. Nothing is
    ever included here automatically -- this is the gate that keeps raw,
    low-confidence scrape data out of the CRM."""
    rows = conn.execute(
        "SELECT h.entity_type, h.entity_id, h.approved_by, h.approved_at, "
        "p.re_number, p.situs_address, p.situs_city, p.situs_state, p.situs_zip, "
        "o.display_name, o.mailing_address, o.mailing_city, o.mailing_state, o.mailing_zip "
        "FROM hubspot_export_flags h "
        "LEFT JOIN properties p ON p.id = h.entity_id AND h.entity_type='property' "
        "LEFT JOIN owners o ON o.id = h.entity_id AND h.entity_type='owner' "
        "WHERE h.approved_by IS NOT NULL AND h.approved_by != ''"
    ).fetchall()

    cols = ["First Name", "Last Name", "Company Name (if entity)", "Mailing Address", "Mailing City",
            "Mailing State", "Mailing Zip", "Property Address", "Property City", "Property State",
            "Property Zip", "Property RE#", "Lead Source", "External Property ID", "External Owner ID",
            "Approved By", "Approved At"]

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            display_name = r["display_name"] or ""
            first, last = ("", "") if not display_name else _split_name(display_name)
            is_entity_name = display_name if (first == "" and last == display_name and " " not in display_name) else ""
            w.writerow([
                first, last, is_entity_name, r["mailing_address"] or "", r["mailing_city"] or "",
                r["mailing_state"] or "", r["mailing_zip"] or "",
                r["situs_address"] or "", r["situs_city"] or "", r["situs_state"] or "", r["situs_zip"] or "",
                r["re_number"] or "", "Duval County Distress Docket",
                r["entity_id"] if r["entity_type"] == "property" else "",
                r["entity_id"] if r["entity_type"] == "owner" else "",
                r["approved_by"] or "", r["approved_at"] or "",
            ])
    return len(rows)
