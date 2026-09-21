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
    merged_into_id TEXT
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

CREATE INDEX IF NOT EXISTS idx_documents_property ON documents(property_id);
CREATE INDEX IF NOT EXISTS idx_documents_owner ON documents(owner_id);
CREATE INDEX IF NOT EXISTS idx_evidence_entity ON evidence(entity_type, entity_id, field_name);
CREATE INDEX IF NOT EXISTS idx_ownerships_owner ON ownerships(owner_id);
"""

TABLES = [
    "properties", "owners", "ownerships", "documents", "evidence",
    "suppressions", "contacts", "overrides", "research_tasks",
    "hubspot_export_flags", "scrape_runs",
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


def init_db(path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
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


def _upsert_property(conn, ts, re_number, situs_address, situs_city, situs_state, situs_zip):
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
            "address_match_confidence=?, last_seen_at=? WHERE id=?",
            (re_number, situs_address, situs_city, situs_state, situs_zip, confidence, ts, prop_id),
        )
    else:
        conn.execute(
            "INSERT INTO properties (id, re_number, situs_address, situs_city, situs_state, "
            "situs_zip, address_match_confidence, first_seen_at, last_seen_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (prop_id, re_number, situs_address, situs_city, situs_state, situs_zip, confidence, ts, ts),
        )
    return prop_id, confidence


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
            "owner_name, other_party, legal_desc, amount, property_id, owner_id, flags_json, "
            "legacy_score, source_name, source_url, scrape_run_id, raw_json, first_seen_at, last_seen_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (doc_id, doc_num, doc_type, r.get("cat", ""), r.get("cat_label", ""), r.get("filed", ""),
             r.get("owner", ""), r.get("grantee", ""), r.get("legal", ""), r.get("amount"),
             prop_id, owner_id, json.dumps(r.get("flags", [])), r.get("score"), source,
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
        "o.display_name, o.mailing_address, o.mailing_city, o.mailing_state, o.mailing_zip, "
        "o.identity_confidence "
        "FROM documents d "
        "LEFT JOIN properties p ON p.id = d.property_id "
        "LEFT JOIN owners o ON o.id = d.owner_id "
        "ORDER BY d.filed_date DESC"
    ).fetchall()

    cols = ["property_id", "owner_id", "First Name", "Last Name", "Owner Display Name",
            "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip", "Owner Identity Confidence",
            "Property RE#", "Property Address", "Property City", "Property State", "Property Zip",
            "Address Match Confidence", "Suppressed", "Category", "Document Type", "Filed Date",
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
