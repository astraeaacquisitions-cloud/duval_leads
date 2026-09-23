"""
Duval County Motivated Seller Lead Tracker -- data generator
--------------------------------------------------------------
Builds records.json (same schema/shape as the Harris County dashboard
this was modeled on) from two live Duval County sources:

  1. Official Records (or.duvalclerk.com) -- pulls the last N days,
     filtered to "distress signal" doc types: Lis Pendens, Notice of Tax
     Deed Sale, Judgment (all variants), Lien, Probate, Notice of
     Commencement, and Release/Satisfaction/Cancellation.
  2. The next scheduled tax deed auction (duval.realtaxdeed.com) -- every
     item on it, i.e. properties about to be sold at auction.

Each run merges into any existing --out file rather than replacing it: a
record already on file (by doc_num) is reused as-is (no repeat PAO lookup)
and carried forward even once it's outside the current --days window, so
records.json accumulates full distress history across runs instead of
only ever showing the last N days. This is what makes it possible to see
how long a property/owner has had distress signals piling up -- a single
run's "filed" dates are all recent by construction, but the accumulated
history's span of filed dates per owner/address is a real "how long have
they been behind" signal. Pass --fresh to rebuild from scratch instead.

Each record is enriched with a property address + mailing address via
the Property Appraiser (paopropertysearch.coj.net):
  - Official Records rows are matched by the GRANTOR name (see the
    county-data-lag caveat in duval_lead_pipeline.py -- same logic reused
    here, including subdivision-based disambiguation for portfolio
    owners).
  - Tax deed items already carry an exact parcel #, so they're looked up
    directly by RE# -- no name search needed.

A 0-100 "motivated seller score" and a set of flags are computed per
record (see compute_score_flags -- the exact rubric is documented there;
it's a judgment call modeled on the patterns visible in the reference
Harris County dashboard, not a scrape of their (invisible, server-side)
scoring code).

CAVEATS carried over from duval_lead_pipeline.py:
  - Grantor-name matches can lag or mismatch for very recently recorded
    documents (county assessment data takes time to catch up).
  - Portfolio owners (many parcels under one name) are disambiguated by
    legal-description/subdivision overlap, which resolves across
    different subdivisions but not between multiple lots in the same one.
  - Neither site offers a public deep link to the actual document image
    (both require login) -- clerk_url points to the relevant search page
    or, for tax deed items, directly to the parcel's Property Appraiser
    page, which IS a real working deep link.

Usage:
    python duval_leads_scraper.py --days 7 --out records.json
"""

import argparse
import datetime
import json
import os
import re
import time

import requests
from bs4 import BeautifulSoup

import duval_leads_db as db

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# ---------------------------------------------------------------------------
# Official Records (or.duvalclerk.com)
# ---------------------------------------------------------------------------

OR_BASE = "https://or.duvalclerk.com"

# DocTypeDescription (as it appears in the grid) -> (cat, cat_label)
CATEGORY_MAP = {
    "LIS PENDENS": ("foreclosure", "Lis Pendens"),
    "NOTICE OF TAX DEED SALE": ("tax", "Notice of Tax Deed Sale"),
    "JUDGMENT": ("judgment", "Judgment"),
    "JUDGMENT/RESTITUTION": ("judgment", "Judgment"),
    "JUDGMENT/SENTENCE": ("judgment", "Judgment"),
    "CC COURT JUDGMENT": ("judgment", "Judgment"),
    "RPO FINAL JUDGMENT": ("judgment", "Judgment"),
    "VA FINAL JUDGMENT": ("judgment", "Judgment"),
    "LIEN": ("lien", "Lien"),
    "PROBATE": ("probate", "Probate Document"),
    "NOTICE COMMENCEMENT": ("construction", "Notice of Commencement"),
    "SATISFACTION": ("release", "Satisfaction / Release"),
    "RELEASE": ("release", "Satisfaction / Release"),
    "CANCELLATION": ("release", "Satisfaction / Release"),
    "PARTIAL RELEASE": ("release", "Satisfaction / Release"),
}

# For most of these doc types, "DirectName" is the party FILING the
# document -- a bank, HOA, debt collector, or the IRS -- not the property
# owner. The actual distressed owner is the OTHER party. Only two
# categories have the owner as DirectName: NOTICE COMMENCEMENT (the
# property owner hires a contractor and files the notice themselves) and
# PROBATE (DirectName is conventionally the decedent, e.g.
# "SMITH JOHN DECEASED", whose estate owns the property).
OWNER_FIELD_BY_CAT = {
    "foreclosure": "IndirectName",   # plaintiff (bank) files against defendant (owner)
    "tax": "IndirectName",           # tax collector notices the delinquent owner
    "judgment": "IndirectName",      # creditor vs. debtor
    "lien": "IndirectName",          # lienholder vs. debtor
    "release": "IndirectName",       # lienholder releasing debtor's lien
    "probate": "DirectName",         # decedent
    "construction": "DirectName",    # owner notices their own contractor
}


def start_or_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    s.get(OR_BASE + "/", timeout=20)
    s.post(OR_BASE + "/search/Disclaimer", data={"Disclaimer": "true"}, timeout=20).raise_for_status()
    return s


def fetch_records_for_day(session, date):
    date_str = f"{date.month}/{date.day}/{date.year}"
    session.get(OR_BASE + "/search/SearchTypeRecordDate", timeout=20)
    session.post(
        OR_BASE + "/search/SearchTypeRecordDate",
        params={"Length": 6},
        data={"RecordDate": date_str},
        timeout=20,
    ).raise_for_status()
    resp = session.post(
        OR_BASE + "/Search/GridResults",
        headers={"X-Requested-With": "XMLHttpRequest"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("Data", [])


def fetch_distress_records(days_back, delay=1.0):
    session = start_or_session()
    today = datetime.date.today()
    out = []
    for i in range(days_back):
        d = today - datetime.timedelta(days=i)
        print(f"[records] Fetching {d.strftime('%m/%d/%Y')} ...")
        try:
            rows = fetch_records_for_day(session, d)
        except Exception as e:
            print(f"  !! failed: {e}")
            rows = []
        kept = [r for r in rows if (r.get("DocTypeDescription") or "").upper() in CATEGORY_MAP]
        print(f"  -> {len(rows)} total, {len(kept)} in tracked categories")
        out.extend(kept)
        time.sleep(delay)
    return out


# ---------------------------------------------------------------------------
# Tax deed auction (duval.realtaxdeed.com)
# ---------------------------------------------------------------------------

TD_BASE = "https://duval.realtaxdeed.com"


def new_taxdeed_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    s.get(TD_BASE + "/", timeout=20)
    return s


def get_next_auction_date(session):
    """Scrape the auction calendar and return the next scheduled tax deed
    sale date (MM/DD/YYYY), or None if nothing is scheduled soon.

    Calendar day cells with a scheduled auction carry a `dayid='MM/DD/YYYY'`
    attribute (days with nothing scheduled have no dayid at all) -- the
    click that opens the auction is handled by client-side JS, not a plain
    href, so we read the attribute directly rather than looking for a link.
    Only checks the currently-displayed month; if nothing is scheduled this
    month, returns None rather than paging forward to the next one."""
    r = session.get(
        TD_BASE + "/index.cfm",
        params={"zaction": "USER", "zmethod": "CALENDAR"},
        timeout=20,
    )
    r.raise_for_status()
    dates = re.findall(r"dayid='(\d{2}/\d{2}/\d{4})'", r.text)
    today = datetime.date.today()
    for d in dates:
        dt = datetime.datetime.strptime(d, "%m/%d/%Y").date()
        if dt >= today:
            return d
    return None


def set_auction_date(session, date_str):
    session.get(
        TD_BASE + "/index.cfm",
        params={"zaction": "AUCTION", "Zmethod": "PREVIEW", "AUCTIONDATE": date_str},
        timeout=20,
    ).raise_for_status()


def load_taxdeed_page(session, page_dir):
    ts = int(time.time() * 1000)
    r = session.get(
        TD_BASE + "/index.cfm",
        params={
            "zaction": "AUCTION", "Zmethod": "UPDATE", "FNC": "LOAD", "AREA": "C",
            "PageDir": page_dir, "doR": 1, "tx": ts, "bypassPage": 0, "test": 1, "_": ts,
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def taxdeed_field(label, block):
    m = re.search(re.escape(label) + r'.*?CAD_DTA">\s*([^@]*)@G', block, re.S)
    return m.group(1).strip() if m else ""


def parse_taxdeed_items(ret_html, auction_date):
    blocks = re.split(r'(?=<div id="AITEM_\d+")', ret_html)
    items = []
    for block in blocks:
        m = re.match(r'<div id="AITEM_(\d+)"', block)
        if not m:
            continue
        parcel_m = re.search(
            r'Parcel ID:.*?CAD_DTA">\s*<a href="([^"]*)"[^>]*>([^<]*)</a>', block, re.S
        )
        parcel_href = parcel_m.group(1) if parcel_m else ""
        parcel_id = parcel_m.group(2).strip() if parcel_m else taxdeed_field("Parcel ID:", block)

        addr_m = re.search(
            r'Property Address:.*?CAD_DTA">\s*([^@]*)@G(.*?)Assessed Value:', block, re.S
        )
        property_address = addr_m.group(1).strip() if addr_m else ""
        city_state_zip = ""
        if addr_m:
            csz_m = re.search(r'CAD_DTA">\s*([^@]*)@G', addr_m.group(2), re.S)
            city_state_zip = csz_m.group(1).strip() if csz_m else ""

        items.append({
            "auction_date": auction_date,
            "case_number": taxdeed_field("Case #:", block),
            "opening_bid": taxdeed_field("Opening Bid:", block),
            "parcel_id": parcel_id,
            "parcel_appraiser_url": parcel_href,
            "property_address": property_address,
            "city_state_zip": city_state_zip,
            "assessed_value": taxdeed_field("Assessed Value:", block),
        })
    return items


def fetch_taxdeed_auction(date_str, delay=0.5, max_pages=200):
    session = new_taxdeed_session()
    set_auction_date(session, date_str)
    seen, all_items, page_dir = set(), [], 0
    for _ in range(max_pages):
        data = load_taxdeed_page(session, page_dir)
        items = parse_taxdeed_items(data.get("retHTML", ""), date_str)
        new_items = [it for it in items if it["parcel_id"] not in seen]
        if not new_items:
            break
        for it in new_items:
            seen.add(it["parcel_id"])
        all_items.extend(new_items)
        page_dir = 1
        time.sleep(delay)
    return all_items


# ---------------------------------------------------------------------------
# Property Appraiser enrichment (paopropertysearch.coj.net)
# ---------------------------------------------------------------------------

PAO_BASE = "https://paopropertysearch.coj.net"

STOPWORDS = {
    "PT", "SEC", "PH", "PHASE", "UNIT", "U", "NO", "AND", "THE",
    "OF", "SUBD", "SD", "REPLAT", "ADDN", "ADDITION", "EST", "ESTATES",
}
NUMBER_WORDS = {
    "ONE": "1", "TWO": "2", "THREE": "3", "FOUR": "4", "FIVE": "5",
    "SIX": "6", "SEVEN": "7", "EIGHT": "8", "NINE": "9", "TEN": "10",
    "ELEVEN": "11", "TWELVE": "12",
}

EXCLUDED_ZIPS = {"32209"}


def zip_excluded(z):
    return any((z or "").strip().startswith(ez) for ez in EXCLUDED_ZIPS)


ENTITY_PATTERN = re.compile(
    # LAND TRUST is kept here (not with the general trust pattern below):
    # investors use a land trust much like an LLC, to hold title without a
    # personal name attached, so it's treated as entity-like for exclusion
    # purposes. A bare "<Name> Trust" is very often an individual's estate
    # plan (living/family/revocable trust) instead -- a legitimate seller,
    # not an entity to filter out -- so it's handled separately by
    # TRUST_PATTERN/is_trust_owned() as a review flag, not an exclusion.
    r"LAND\s*TRUST|\bLLC\b|\bL\.L\.C\.?\b|\bINC\.?\b|\bCORP(?:ORATION)?\.?\b|"
    r"\bLP\b|\bLLP\b|\bLTD\.?\b|\bCOMPANY\b|\bHOLDINGS\b|\bENTERPRISES\b|"
    r"\bINVESTMENTS?\s*(GROUP)?\b|\bPROPERTIES\b|\bCAPITAL\b|\bVENTURES?\b",
    re.I,
)

# Trusts are NOT excluded (revocable/living/family trusts are a common,
# legitimate estate-planning vehicle and can be exactly the probate-
# adjacent motivated-seller situation this system exists to find). They're
# flagged for manual authority verification instead -- see
# TRUST_OWNERSHIP_REVIEW_REQUIRED in compute_score_flags. Do not assume a
# beneficiary, relative, occupant, or associated person has authority to
# sell just because their name is attached to the trust.
TRUST_PATTERN = re.compile(r"\bTRUST\b", re.I)


def is_entity_owned(owner_name):
    return bool(ENTITY_PATTERN.search(owner_name or ""))


def is_trust_owned(owner_name):
    return bool(TRUST_PATTERN.search(owner_name or ""))


def is_unit_address(addr):
    return bool(re.search(r"\bUNIT\b", addr or "", re.I))


# ---------------------------------------------------------------------------
# Property-type classification (Phase 2 acquisition criteria)
# ---------------------------------------------------------------------------

PROPERTY_TYPE_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "property_type_rules.json"
)


def load_property_type_config(path=PROPERTY_TYPE_CONFIG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


PROPERTY_TYPE_CONFIG = load_property_type_config()


def classify_property_type(property_use_code, config=None):
    """Classifies a 4-digit Florida DOR Property Use code (e.g. "0100" for
    Single Family) into ('include' | 'review' | 'exclude', label), driven
    entirely by config/property_type_rules.json -- edit that file to
    change what's in/out/flagged, no code change needed. An unrecognized
    prefix defaults to 'review', never silently excluded or included: a
    genuinely new/rare code should get a human look, not vanish or get
    waved through."""
    config = config if config is not None else PROPERTY_TYPE_CONFIG
    prefix = (property_use_code or "")[:2]
    label = config.get("labels", {}).get(prefix, prefix or "Unknown")
    if not prefix:
        return config.get("default_decision", "review"), "Unknown"

    decisions = config.get("decisions", {})
    if prefix in decisions:
        return decisions[prefix], label

    try:
        n = int(prefix)
    except ValueError:
        return config.get("default_decision", "review"), label
    for lo, hi in config.get("exclude_ranges", []):
        if lo <= n <= hi:
            return "exclude", label
    return config.get("default_decision", "review"), label


def property_type_excluded(record, include_all_property_types=False):
    """Standalone, testable version of the property-type check used in
    build_records()'s exclusion filter -- true only for records already
    classified 'exclude' by classify_property_type(). Missing/None
    decisions (records predating Phase 2) are never excluded by this."""
    if include_all_property_types:
        return False
    return record.get("property_type_decision") == "exclude"


def is_teardown_infill_candidate(property_use_code, year_built, building_count, config=None):
    """A signal flag, not a score -- see TEARDOWN_INFILL_CANDIDATE. Vacant
    residential land, or a single old structure, are the simple heuristics;
    this makes no claim about condition or actual redevelopment feasibility."""
    config = config if config is not None else PROPERTY_TYPE_CONFIG
    td = config.get("teardown_infill", {})
    prefix = (property_use_code or "")[:2]
    if prefix in td.get("vacant_prefixes", ["00"]):
        return True
    threshold = td.get("old_building_year_threshold", 1960)
    if year_built and (building_count or 0) <= 1 and year_built < threshold:
        return True
    return False


# Duval's Official Records has no distinct "Code Violation" doc type --
# code enforcement liens are just filed as generic LIEN. The only way to
# separate them out is by WHO filed the lien: if the filer (the "grantee"
# field for a lien record -- see OWNER_FIELD_BY_CAT) looks like a city/
# county/code-enforcement entity rather than a bank, HOA, or contractor,
# treat it as a likely code violation lien.
MUNICIPAL_FILER_PATTERN = re.compile(
    r"\bCITY OF\b|\bCONSOLIDATED CITY\b|\bCODE ENFORCEMENT\b|"
    r"\bDUVAL COUNTY\b|\bCOUNTY OF DUVAL\b|\bMUNICIPAL\b|"
    r"\bCITY OF JACKSONVILLE\b",
    re.I,
)

# Phase 6: not every municipal filer is a code violation specifically --
# "CITY OF"/"DUVAL COUNTY" could just as easily be a demolition lien, a
# nuisance-abatement lien, or an unpaid-utility lien. Only a filer name
# that literally names code enforcement/compliance is unambiguous; the
# broader MUNICIPAL_FILER_PATTERN match above is real signal but weaker,
# so both get flagged as a likely code violation lien (unchanged
# behavior) while this distinguishes how confident that specific label is.
CODE_ENFORCEMENT_STRONG_PATTERN = re.compile(
    r"\bCODE ENFORCEMENT\b|\bCODE COMPLIANCE\b|\bMUNICIPAL CODE\b",
    re.I,
)


def new_pao_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def get_hidden_fields(html):
    fields = {}
    for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__PREVIOUSPAGE", "__EVENTVALIDATION"):
        m = re.search(rf'id="{name}" value="([^"]*)"', html)
        fields[name] = m.group(1) if m else ""
    return fields


def normalize_tokens(text):
    text = (text or "").upper()
    text = re.sub(r"^\s*\d{3,6}\s+", " ", text)
    text = re.sub(r"\bPT\b", " ", text)
    text = re.sub(r"\bLOTS?\.?\s*\d+\b", " ", text)
    text = re.sub(r"\bL\.?\s*\d+\b", " ", text)
    text = re.sub(r"\bBLOCKS?\.?\s*\d+\b", " ", text)
    text = re.sub(r"\bBLK\.?\s*\d+\b", " ", text)
    text = re.sub(r"\bB\.?\s*\d+\b", " ", text)
    text = re.sub(r"\bSEC\.?\s*\d+[-\s]+\d+[NSEW]?[-\s]+\d+[NSEW]?\b", " ", text)
    text = re.sub(r"&\s*C\b", " ", text)
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    tokens = set()
    for raw in text.split():
        t = NUMBER_WORDS.get(raw, raw)
        if t in STOPWORDS:
            continue
        if t.isdigit():
            tokens.add(str(int(t)))
            continue
        if len(t) < 3:
            continue
        tokens.add(t)
    return tokens


def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def search_owner(session, name, results_per_page="2000"):
    r = session.get(PAO_BASE + "/Basic/Search.aspx", timeout=20)
    r.raise_for_status()
    hidden = get_hidden_fields(r.text)
    data = {
        "__LASTFOCUS": "", "__EVENTTARGET": "", "__EVENTARGUMENT": "",
        "__VIEWSTATE": hidden["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": hidden["__VIEWSTATEGENERATOR"],
        "__PREVIOUSPAGE": hidden["__PREVIOUSPAGE"],
        "__EVENTVALIDATION": hidden["__EVENTVALIDATION"],
        "ctl00$cphBody$tbRE6": "", "ctl00$cphBody$tbRE4": "",
        "ctl00$cphBody$tbName": name,
        "ctl00$cphBody$tbStreetNumber": "", "ctl00$cphBody$tbStreetName": "",
        "ctl00$cphBody$ddStreetSuffix": "", "ctl00$cphBody$ddStreetPrefix": "",
        "ctl00$cphBody$tbStreetUnit": "", "ctl00$cphBody$ddCity": "",
        "ctl00$cphBody$tbZipCode": "", "ctl00$cphBody$ddSearchType": "All",
        "ctl00$cphBody$ddResultsPerPage": results_per_page,
        "ctl00$cphBody$bSearch": "Search",
    }
    r2 = session.post(PAO_BASE + "/Basic/Results.aspx", data=data, timeout=30)
    r2.raise_for_status()
    soup = BeautifulSoup(r2.text, "html.parser")
    table = soup.find("table", id="ctl00_cphBody_gridResults")
    if table is None:
        return []
    results = []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if len(cells) < 9:
            continue
        re_link = cells[0].find("a")
        re_raw = re_link["href"].split("RE=")[-1] if re_link and "RE=" in re_link.get("href", "") else ""
        street_parts = [cells[2].get_text(strip=True), cells[3].get_text(strip=True),
                        cells[4].get_text(strip=True), cells[5].get_text(strip=True)]
        situs_address = " ".join(p for p in street_parts if p and p != "\xa0")
        unit = cells[6].get_text(strip=True)
        if unit and unit != "\xa0":
            situs_address += f" UNIT {unit}"
        results.append({
            "re_number": cells[0].get_text(strip=True),
            "re_raw": re_raw,
            "situs_address": situs_address,
            "situs_city": cells[7].get_text(strip=True),
            "situs_zip": cells[8].get_text(strip=True),
        })
    return results


def get_detail(session, re_raw):
    r = session.get(PAO_BASE + f"/Basic/Detail.aspx?RE={re_raw}", timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    def text(id_):
        el = soup.find(id=id_)
        return el.get_text(strip=True) if el else ""

    subdivision = ""
    sub_label = soup.find(string=re.compile(r"^\s*Subdivision\s*$"))
    if sub_label:
        row = sub_label.find_parent("tr")
        if row:
            cells = row.find_all("td")
            if cells:
                subdivision = cells[-1].get_text(strip=True)

    mailing_name = text("ctl00_cphBody_repeaterOwnerInformation_ctl00_lblOwnerName")
    raw_lines = [
        text("ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine1"),
        text("ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine2"),
        text("ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine3"),
    ]
    lines = [l for l in raw_lines if l]
    # The last non-empty line is always city/state/zip; everything before
    # it is the mailing address itself (which can be two lines, e.g. a
    # "C/O ..." line followed by the actual street).
    if len(lines) >= 2:
        mailing_address = ", ".join(lines[:-1])
        mailing_csz = lines[-1]
    elif len(lines) == 1:
        mailing_address = ""
        mailing_csz = lines[0]
    else:
        mailing_address = ""
        mailing_csz = ""

    def parse_money(s):
        if not s:
            return None
        try:
            return float(s.replace("$", "").replace(",", ""))
        except ValueError:
            return None

    def money_field(in_progress_id, certified_id):
        # Prefer the current working-roll value; some fields (e.g. Taxable
        # Value's InProgress cell) read "See below" instead of a number
        # for part of the year, so fall back to the last certified figure
        # rather than silently returning nothing.
        return parse_money(text(in_progress_id)) if parse_money(text(in_progress_id)) is not None \
            else parse_money(text(certified_id))

    # Just (Market) Value and Assessed Value are genuinely different in FL
    # once Save-Our-Homes/non-homestead caps apply -- never treat one as a
    # stand-in for the other. See PROPERTY_TYPE_CONFIG-style caveat: PAO
    # values are a county tax-roll figure, not a verified current market
    # value (surfaced to the dashboard, not silently assumed here).
    market_value = money_field("ctl00_cphBody_lblJustMarketValueInProgress", "ctl00_cphBody_lblJustMarketValueCertified")
    assessed_value = money_field("ctl00_cphBody_lblAssessedValueA10InProgress", "ctl00_cphBody_lblAssessedValueA10Certified")
    taxable_value = money_field("ctl00_cphBody_lblTaxableValueInProgress", "ctl00_cphBody_lblTaxableValueCertified")

    last_sale_date = None
    last_sale_price = None
    sales_table = soup.find(id="ctl00_cphBody_gridSalesHistory")
    if sales_table:
        data_rows = sales_table.find_all("tr")[1:]
        if data_rows:
            cells = data_rows[0].find_all("td")
            if len(cells) >= 3:
                date_str = cells[1].get_text(strip=True)
                try:
                    m, d, y = date_str.split("/")
                    last_sale_date = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
                except ValueError:
                    pass
                price_str = cells[2].get_text(strip=True)
                try:
                    last_sale_price = float(price_str.replace("$", "").replace(",", ""))
                except ValueError:
                    pass

    # Property Use is the Florida DOR use code for the parcel as a whole
    # (e.g. "0100 Single Family") -- the authoritative field for
    # residential/apartment/commercial classification, and it's on this
    # same page already fetched for market value/sales history, so this
    # costs no extra request. See classify_property_type().
    property_use_code, property_use_label = "", ""
    use_str = text("ctl00_cphBody_lblPropertyUse")
    use_m = re.match(r"^\s*(\d{4})\s+(.*)$", use_str)
    if use_m:
        property_use_code, property_use_label = use_m.group(1), use_m.group(2).strip()
    elif use_str:
        property_use_label = use_str

    # Buildings are a repeater (ctl00, ctl01, ...); zero matches is itself
    # a signal (vacant land). Take the first building's type/year as the
    # primary structure -- same "most recent/primary row" convention as
    # last_sale_date/last_sale_price above.
    building_type_els = soup.find_all(id=re.compile(r"^ctl00_cphBody_repeaterBuilding_ctl\d+_lblBuildingType$"))
    year_built_els = soup.find_all(id=re.compile(r"^ctl00_cphBody_repeaterBuilding_ctl\d+_lblYearBuilt$"))
    building_count = len(building_type_els)
    building_type = building_type_els[0].get_text(strip=True) if building_type_els else ""
    year_built = None
    if year_built_els:
        try:
            year_built = int(year_built_els[0].get_text(strip=True))
        except ValueError:
            pass

    return {
        "subdivision": subdivision,
        "mailing_name": mailing_name,
        "mailing_address": mailing_address,
        "mailing_city_state_zip": mailing_csz,
        "market_value": market_value,
        "assessed_value": assessed_value,
        "taxable_value": taxable_value,
        "last_sale_date": last_sale_date,
        "last_sale_price": last_sale_price,
        "property_use_code": property_use_code,
        "property_use_label": property_use_label,
        "building_count": building_count,
        "building_type": building_type,
        "year_built": year_built,
    }


def resolve_owner(session, name, legal_desc, max_detail_scan=40, delay=0.4):
    candidates = search_owner(session, name)
    time.sleep(delay)
    if not candidates:
        return None
    if len(candidates) == 1:
        detail = {}
        try:
            detail = get_detail(session, candidates[0]["re_raw"])
        except Exception:
            pass
        time.sleep(delay)
        return {**candidates[0], **detail}

    deed_tokens = normalize_tokens(legal_desc)
    scan = candidates[:max_detail_scan]
    scored = []
    for c in scan:
        try:
            detail = get_detail(session, c["re_raw"])
        except Exception:
            detail = {}
        time.sleep(delay)
        merged = {**c, **detail}
        score = jaccard(deed_tokens, normalize_tokens(merged.get("subdivision", "")))
        scored.append((score, merged))
    scored.sort(key=lambda x: -x[0])
    return scored[0][1]


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

BASE_SCORE = {
    "foreclosure": 70,
    "tax": 70,
    "judgment": 45,
    "lien": 35,
    "code_violation": 50,
    "probate": 55,
    "construction": 15,
    "release": 5,
}


def parse_city_state_zip(csz):
    """Split 'JACKSONVILLE, FL 32218-2857' (or a trailing-dash zip with no
    +4, e.g. '32244-') into (city, state, zip)."""
    if not csz:
        return "", "", ""
    m = re.match(r"^\s*(.*?),\s*([A-Z]{2})\s*([\d-]*)\s*$", csz.upper())
    if not m:
        return csz.strip(), "", ""
    city, state, zip_ = m.groups()
    return city.strip().title(), state, zip_.rstrip("-")


def street_norm(addr):
    s = (addr or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    repl = {"ROAD": "RD", "STREET": "ST", "AVENUE": "AVE", "DRIVE": "DR", "LANE": "LN",
            "COURT": "CT", "CIRCLE": "CIR", "BOULEVARD": "BLVD", "PLACE": "PL",
            "TERRACE": "TER", "TRAIL": "TRL", "PARKWAY": "PKWY", "HIGHWAY": "HWY"}
    words = [repl.get(w, w) for w in s.split()]
    return " ".join(words)


def compute_score_flags(cat, filed_date, owner_name, prop_address, mail_address, is_multi_party):
    score = BASE_SCORE.get(cat, 20)
    flags = []

    if filed_date:
        try:
            fdt = datetime.datetime.strptime(filed_date, "%Y-%m-%d").date()
            if (datetime.date.today() - fdt).days <= 7:
                flags.append("New this week")
                score += 10
        except ValueError:
            pass

    if ENTITY_PATTERN.search(owner_name or ""):
        flags.append("LLC / corp owner")

    if is_trust_owned(owner_name):
        flags.append("TRUST_OWNERSHIP_REVIEW_REQUIRED")

    if prop_address and mail_address:
        p, m = street_norm(prop_address), street_norm(mail_address)
        if p and p not in m:
            flags.append("Absentee owner")
            score += 15

    if is_multi_party:
        flags.append("Multiple parties")
        score += 10

    return min(score, 100), flags


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def build_records(days_back, delay_records, delay_pao, max_detail_scan, history=None,
                   include_all_property_types=False):
    """history maps doc_num -> a previously-built record (loaded from a prior
    run's output). A doc_num already in history is reused as-is instead of
    re-enriched (the filing itself is immutable once recorded, and this
    saves a PAO lookup); any history doc_num not encountered in this run's
    fetch window is carried forward unchanged so old records.json entries
    outside the last `days_back` days aren't lost. Every record -- reused,
    freshly enriched, or carried forward -- is re-run through the current
    exclusion filters, so a filtering-rule change also cleans up history."""
    history = history or {}
    session = new_pao_session()
    records = []
    seen_doc_nums = set()

    def keep(record):
        return (
            not zip_excluded(record["prop_zip"])
            and not is_entity_owned(record["owner"])
            and not is_unit_address(record["prop_address"])
            and not property_type_excluded(record, include_all_property_types)
        )

    def take(doc_num, record):
        if not doc_num or doc_num in seen_doc_nums:
            if doc_num:
                print(f"[dedup] skipped duplicate doc_num {doc_num}")
            return
        seen_doc_nums.add(doc_num)
        if keep(record):
            records.append(record)

    # -- Official Records --
    raw = fetch_distress_records(days_back, delay=delay_records)
    print(f"[records] {len(raw)} distress-category official records to enrich")
    cache = {}
    new_count = 0
    for i, r in enumerate(raw, 1):
        doc_num = r.get("InstrumentNumber", "")
        if doc_num and doc_num in history:
            take(doc_num, history[doc_num])
            continue
        new_count += 1

        cat, cat_label = CATEGORY_MAP.get((r.get("DocTypeDescription") or "").upper(), ("other", r.get("DocTypeDescription", "")))
        owner_field = OWNER_FIELD_BY_CAT.get(cat, "DirectName")
        other_field = "IndirectName" if owner_field == "DirectName" else "DirectName"

        name = r.get(owner_field, "")
        legal = r.get("DocLegalDescription", "")
        # The county doesn't give a stable per-owner match: two filings under the same
        # name can be for different parcels, and only the legal description tells them
        # apart, so the cache must be keyed on both.
        cache_key = (name, legal)
        if cache_key not in cache:
            print(f"[pao {i}/{len(raw)}] ({cat}) {name}")
            try:
                cache[cache_key] = resolve_owner(session, name, legal, max_detail_scan, delay_pao)
            except Exception as e:
                print(f"  !! PAO lookup failed, skipping match: {e}")
                cache[cache_key] = None
        match = cache[cache_key] or {}

        owner_full = name
        other_party = r.get(other_field, "")
        filed = r.get("RecordDate", "").replace("/", "-")
        is_multi = " GRANTOR " in owner_full.upper() or len(owner_full.split("/")) > 1

        is_code_violation = cat == "lien" and MUNICIPAL_FILER_PATTERN.search(other_party or "")
        code_violation_confidence = None
        if is_code_violation:
            cat, cat_label = "code_violation", "Possible Code Violation Lien"
            code_violation_confidence = "STRONG" if CODE_ENFORCEMENT_STRONG_PATTERN.search(other_party or "") else "MODERATE"

        prop_address = match.get("situs_address", "")
        prop_city = match.get("situs_city", "")
        mail_address = match.get("mailing_address", "")
        mail_city, mail_state, mail_zip = parse_city_state_zip(match.get("mailing_city_state_zip", ""))

        score, flags = compute_score_flags(cat, filed, owner_full, prop_address, mail_address, is_multi)
        if is_code_violation:
            flags.append("Filed by city/county")

        last_sale_date = match.get("last_sale_date")
        years_owned = None
        if last_sale_date:
            try:
                sale_year = int(last_sale_date[:4])
                years_owned = datetime.date.today().year - sale_year
            except (ValueError, TypeError):
                pass

        property_type_decision, property_type_label = classify_property_type(match.get("property_use_code", ""))
        if property_type_decision == "review":
            flags.append("PROPERTY_TYPE_REVIEW_REQUIRED")
        if is_teardown_infill_candidate(match.get("property_use_code", ""), match.get("year_built"), match.get("building_count")):
            flags.append("TEARDOWN_INFILL_CANDIDATE")
        if cat == "probate":
            # DirectName here is conventionally the decedent (e.g. "SMITH
            # JOHN DECEASED") -- a legitimate estate-sale lead, but never
            # a contactable person. Same discipline as trust ownership:
            # flag for review, never assume a relative/heir/occupant has
            # authority to sell without confirming the actual personal
            # representative.
            flags.append("PROBATE_REPRESENTATIVE_REVIEW_REQUIRED")

        record = {
            "doc_num": doc_num,
            "doc_type": r.get("DocTypeDescription", ""),
            "filed": filed,
            "cat": cat,
            "cat_label": cat_label,
            "owner": owner_full,
            "grantee": other_party,
            "amount": None,
            "legal": legal,
            "prop_address": prop_address,
            "prop_city": prop_city,
            "prop_state": "FL",
            "prop_zip": match.get("situs_zip", "").rstrip("-"),
            "mail_address": mail_address,
            "mail_city": mail_city,
            "mail_state": mail_state,
            "mail_zip": mail_zip,
            "clerk_url": "https://or.duvalclerk.com/search/SearchTypeInstrumentNumber",
            "re_number": match.get("re_number", "") or match.get("re_raw", ""),
            "property_use_code": match.get("property_use_code", ""),
            "property_type_label": property_type_label,
            "property_type_decision": property_type_decision,
            "year_built": match.get("year_built"),
            "building_count": match.get("building_count"),
            "flags": flags,
            "score": score,
            "source": "Duval County Clerk -- Official Records",
            "market_value": match.get("market_value"),
            "assessed_value": match.get("assessed_value"),
            "taxable_value": match.get("taxable_value"),
            "last_sale_date": last_sale_date,
            "last_sale_price": match.get("last_sale_price"),
            "years_owned": years_owned,
            "code_violation_confidence": code_violation_confidence,
        }
        take(doc_num, record)
    print(f"[records] {new_count} newly enriched, {len(raw) - new_count} reused from history")

    # -- Tax deed auction --
    auction_date = None
    try:
        td_session = new_taxdeed_session()
        auction_date = get_next_auction_date(td_session)
    except Exception as e:
        print(f"[taxdeed] Skipping tax deed auction -- site unreachable: {e}")
    if auction_date:
        print(f"[taxdeed] Next auction: {auction_date}")
        items = fetch_taxdeed_auction(auction_date)
        print(f"[taxdeed] {len(items)} items to enrich")
        for i, item in enumerate(items, 1):
            doc_num = item["case_number"]
            if doc_num and doc_num in history:
                take(doc_num, history[doc_num])
                continue

            re_raw = item["parcel_id"].replace("-", "")
            print(f"[pao {i}/{len(items)}] RE# {item['parcel_id']}")
            detail = {}
            if re_raw:
                try:
                    detail = get_detail(session, re_raw)
                except Exception:
                    pass
                time.sleep(delay_pao)

            owner_name = detail.get("mailing_name", "")
            mail_address = detail.get("mailing_address", "")
            mail_city, mail_state, mail_zip = parse_city_state_zip(detail.get("mailing_city_state_zip", ""))
            score, flags = compute_score_flags(
                "tax", None, owner_name, item["property_address"], mail_address, False
            )
            flags.append("Tax deed sale scheduled")

            city = item["city_state_zip"].split(",")[0].strip() if "," in item["city_state_zip"] else ""
            zipm = re.search(r"(\d{5})", item["city_state_zip"])

            last_sale_date = detail.get("last_sale_date")
            years_owned = None
            if last_sale_date:
                try:
                    sale_year = int(last_sale_date[:4])
                    years_owned = datetime.date.today().year - sale_year
                except (ValueError, TypeError):
                    pass

            property_type_decision, property_type_label = classify_property_type(detail.get("property_use_code", ""))
            if property_type_decision == "review":
                flags.append("PROPERTY_TYPE_REVIEW_REQUIRED")
            if is_teardown_infill_candidate(detail.get("property_use_code", ""), detail.get("year_built"), detail.get("building_count")):
                flags.append("TEARDOWN_INFILL_CANDIDATE")

            record = {
                "doc_num": doc_num,
                "doc_type": "TAX DEED",
                "filed": auction_date.replace("/", "-") if auction_date else "",
                "cat": "tax",
                "cat_label": "Tax Deed",
                "owner": owner_name,
                "grantee": "",
                "amount": item["opening_bid"].replace("$", "").replace(",", "") or None,
                "legal": "",
                "prop_address": item["property_address"],
                "prop_city": city,
                "prop_state": "FL",
                "prop_zip": zipm.group(1) if zipm else "",
                "mail_address": mail_address,
                "mail_city": mail_city,
                "mail_state": mail_state,
                "mail_zip": mail_zip,
                "clerk_url": item["parcel_appraiser_url"],
                "re_number": item["parcel_id"],
                "property_use_code": detail.get("property_use_code", ""),
                "property_type_label": property_type_label,
                "property_type_decision": property_type_decision,
                "year_built": detail.get("year_built"),
                "building_count": detail.get("building_count"),
                "flags": flags,
                "score": score,
                "source": "Duval County Tax Deed Auction",
                "market_value": detail.get("market_value"),
                "assessed_value": detail.get("assessed_value"),
                "taxable_value": detail.get("taxable_value"),
                "last_sale_date": last_sale_date,
                "last_sale_price": detail.get("last_sale_price"),
                "years_owned": years_owned,
            }
            take(doc_num, record)

    # Carry forward history records untouched this run -- e.g. aged out of
    # the fetch window, or the tax deed site was unreachable -- so previously
    # discovered leads aren't lost just because they didn't reappear today.
    carried = 0
    for doc_num, record in history.items():
        if doc_num in seen_doc_nums:
            continue
        seen_doc_nums.add(doc_num)
        if keep(record):
            records.append(record)
            carried += 1
    print(f"[history] carried forward {carried} previously-seen records outside this run's fetch window")

    return records


def enrich_records_with_ids(conn, records, run_id=None):
    """Looks up the property_id/owner_id that persist_records() just
    assigned (deterministically, from the DB module -- the single source
    of truth for identity), plus the confidence labels and Phase 3
    valuation/dealability results, and merges them back into the
    in-memory record dicts as additive fields. This does not touch,
    reorder, or filter `records` -- it only adds keys -- so it has no
    effect on the accumulate-by-doc_num history mechanism records.json
    depends on."""
    doc_rows = conn.execute(
        "SELECT source_name, doc_num, doc_type, property_id, owner_id, code_violation_confidence FROM documents"
    ).fetchall()
    prop_conf = {r["id"]: r["address_match_confidence"] for r in conn.execute(
        "SELECT id, address_match_confidence FROM properties")}
    owner_conf = {r["id"]: r["identity_confidence"] for r in conn.execute(
        "SELECT id, identity_confidence FROM owners")}
    valuations = {r["property_id"]: r for r in conn.execute(
        "SELECT property_id, pao_assessed_value, pao_market_value, pao_taxable_value, "
        "value_confidence, equity_signal, equity_confidence, equity_reasoning, "
        "active_lien_count, has_tax_distress, mortgage_estimate, "
        "tax_deed_stage, days_until_tax_sale, "
        "foreclosure_stage, days_since_foreclosure_milestone, "
        "code_enforcement_stage, code_violation_count, "
        "absentee_stage, landlord_portfolio_stage, portfolio_property_count, portfolio_distressed_count "
        "FROM valuations")}
    scores = {}
    if run_id:
        scores = {r["property_id"]: r for r in conn.execute(
            "SELECT property_id, dealability_score, dealability_reason, "
            "distress_score, distress_reason, urgency_score, urgency_reason, "
            "confidence_score, confidence_reason, contact_priority_score, tier, tier_reason, outreach_angle "
            "FROM scores WHERE scrape_run_id=?",
            (run_id,))}
    by_key = {(r["source_name"], r["doc_num"], r["doc_type"]): (r["property_id"], r["owner_id"]) for r in doc_rows}
    code_conf_by_key = {(r["source_name"], r["doc_num"], r["doc_type"]): r["code_violation_confidence"] for r in doc_rows}

    for r in records:
        prop_id, owner_id = by_key.get((r.get("source", ""), r.get("doc_num", ""), r.get("doc_type", "")), (None, None))
        r["property_id"] = prop_id
        r["owner_id"] = owner_id
        r["address_match_confidence"] = prop_conf.get(prop_id, "unmatched")
        r["owner_identity_confidence"] = owner_conf.get(owner_id, "unmatched")

        # Probate/estate language in the owner name (see
        # db.is_probate_estate_name) -- backfilled here, not only in
        # db.persist_records, so a record already sitting in records.json
        # from before this check existed also gets flagged for the
        # dashboard's Review Required Only filter on the next run, without
        # needing to be re-scraped.
        if db.is_probate_estate_name(r.get("owner", "")) and \
                "PROBATE_REPRESENTATIVE_REVIEW_REQUIRED" not in r.get("flags", []):
            r.setdefault("flags", []).append("PROBATE_REPRESENTATIVE_REVIEW_REQUIRED")

        val = valuations.get(prop_id)
        r["pao_assessed_value"] = val["pao_assessed_value"] if val else None
        r["pao_market_value"] = val["pao_market_value"] if val else None
        r["pao_taxable_value"] = val["pao_taxable_value"] if val else None
        r["value_confidence"] = val["value_confidence"] if val else "UNKNOWN"
        r["equity_signal"] = val["equity_signal"] if val else "UNKNOWN"
        r["equity_confidence"] = val["equity_confidence"] if val else "UNKNOWN"
        r["equity_reasoning"] = val["equity_reasoning"] if val else ""
        r["active_lien_count"] = val["active_lien_count"] if val else None
        r["mortgage_estimate"] = val["mortgage_estimate"] if val else "UNKNOWN"
        r["tax_deed_stage"] = val["tax_deed_stage"] if val and val["tax_deed_stage"] else "TAX_STAGE_NONE"
        r["days_until_tax_sale"] = val["days_until_tax_sale"] if val else None
        r["foreclosure_stage"] = val["foreclosure_stage"] if val and val["foreclosure_stage"] else "FORECLOSURE_STAGE_NONE"
        r["days_since_foreclosure_milestone"] = val["days_since_foreclosure_milestone"] if val else None
        r["code_enforcement_stage"] = val["code_enforcement_stage"] if val and val["code_enforcement_stage"] else "CODE_STAGE_NONE"
        r["code_violation_count"] = val["code_violation_count"] if val else None
        r["code_violation_confidence"] = code_conf_by_key.get((r.get("source", ""), r.get("doc_num", ""), r.get("doc_type", "")))
        if r["code_enforcement_stage"] == "CODE_STAGE_REPEAT" and "CHRONIC_CODE_VIOLATIONS" not in r.get("flags", []):
            r.setdefault("flags", []).append("CHRONIC_CODE_VIOLATIONS")

        r["absentee_stage"] = val["absentee_stage"] if val and val["absentee_stage"] else "ABSENTEE_STAGE_UNKNOWN"
        r["landlord_portfolio_stage"] = val["landlord_portfolio_stage"] if val and val["landlord_portfolio_stage"] else "LANDLORD_STAGE_UNKNOWN"
        r["portfolio_property_count"] = val["portfolio_property_count"] if val else None
        r["portfolio_distressed_count"] = val["portfolio_distressed_count"] if val else None
        if r["absentee_stage"] == "ABSENTEE_STAGE_OUT_OF_STATE" and "OUT_OF_STATE_OWNER" not in r.get("flags", []):
            r.setdefault("flags", []).append("OUT_OF_STATE_OWNER")
        if r["landlord_portfolio_stage"] == "LANDLORD_STAGE_MULTI_PROPERTY_DISTRESS" and "LANDLORD_MULTI_PROPERTY_DISTRESS" not in r.get("flags", []):
            r.setdefault("flags", []).append("LANDLORD_MULTI_PROPERTY_DISTRESS")

        sc = scores.get(prop_id)
        r["dealability_score"] = sc["dealability_score"] if sc else None
        r["dealability_reason"] = sc["dealability_reason"] if sc else ""
        r["distress_score"] = sc["distress_score"] if sc else None
        r["distress_reason"] = sc["distress_reason"] if sc else ""
        r["urgency_score"] = sc["urgency_score"] if sc else None
        r["urgency_reason"] = sc["urgency_reason"] if sc else ""
        r["confidence_score"] = sc["confidence_score"] if sc else None
        r["confidence_reason"] = sc["confidence_reason"] if sc else ""
        r["contact_priority_score"] = sc["contact_priority_score"] if sc else None
        r["tier"] = sc["tier"] if sc else None
        r["tier_reason"] = sc["tier_reason"] if sc else ""
        r["outreach_angle"] = sc["outreach_angle"] if sc else ""
    return records


def main():
    parser = argparse.ArgumentParser(description="Build records.json for the Duval County lead tracker dashboard")
    parser.add_argument("--days", type=int, default=7, help="Days of Official Records to pull (default 7)")
    parser.add_argument("--records-delay", type=float, default=1.0)
    parser.add_argument("--pao-delay", type=float, default=0.4)
    parser.add_argument("--max-detail-scan", type=int, default=40)
    parser.add_argument("--out", default="records.json")
    parser.add_argument("--fresh", action="store_true",
                         help="Ignore any existing --out file and rebuild from scratch instead of merging with prior history")
    parser.add_argument("--db", default="data/duval_leads.db",
                         help="Structured research/scoring SQLite layer (additive -- see duval_leads_db.py's "
                              "module docstring for data ownership). Rebuilt from --backup on each run.")
    parser.add_argument("--backup", default="data/backup.json",
                         help="Full-fidelity export of the SQLite layer, git-tracked for durability")
    parser.add_argument("--csv", default="data/export_leads.csv")
    parser.add_argument("--hubspot-csv", default="data/hubspot_export.csv")
    parser.add_argument("--include-all-property-types", action="store_true",
                         help="Disable the Phase 2 apartment/commercial exclusion filter entirely "
                              "(the \"manually enable\" escape hatch -- normally edit "
                              "config/property_type_rules.json instead, which is reversible per-category)")
    parser.add_argument("--skip-db", action="store_true",
                         help="Scrape and write records.json only, skip the SQLite layer (debugging escape hatch)")
    args = parser.parse_args()

    history = {}
    if not args.fresh:
        try:
            with open(args.out, "r", encoding="utf-8") as f:
                prior = json.load(f)
            for rec in prior.get("records", []):
                doc_num = rec.get("doc_num")
                if doc_num:
                    history[doc_num] = rec
            print(f"[history] loaded {len(history)} previously-seen records from {args.out}")
        except FileNotFoundError:
            pass
        except (json.JSONDecodeError, OSError) as e:
            print(f"[history] could not load prior {args.out}, starting fresh: {e}")

    # `records` is the FULL accumulated set -- freshly enriched, reused
    # from history, and carried-forward -- exactly as build_records()
    # already assembles it. Nothing below this point changes that list's
    # membership or the doc_num-keyed history mechanism; the SQLite layer
    # only reads it and adds identity/confidence fields on top.
    records = build_records(args.days, args.records_delay, args.pao_delay, args.max_detail_scan,
                             history=history, include_all_property_types=args.include_all_property_types)

    conn = None
    if not args.skip_db:
        conn = db.init_db(args.db)
        if os.path.exists(args.backup):
            n = db.load_backup_into_db(conn, args.backup)
            print(f"[db] Rehydrated from {args.backup} ({n} rows across all tables)")
        elif db.is_empty(conn):
            n = db.seed_from_records_json(conn, args.out) if os.path.exists(args.out) else 0
            if n:
                print(f"[db] First run -- seeded {n} legacy records from {args.out}")

        run_id = db.start_run(conn, sources_run="official_records,tax_deed_auction")
        n_persisted = db.persist_records(conn, records, run_id)
        db.finish_run(conn, run_id, n_persisted, status="ok")
        print(f"[db] Persisted {n_persisted} records this run (full accumulated history, not just today's pull)")

        n_scored = db.compute_dealability_for_all_properties(conn, run_id)
        print(f"[db] Computed Dealability Score for {n_scored} properties")

        records = enrich_records_with_ids(conn, records, run_id=run_id)

    today = datetime.date.today()
    fetch_start = today - datetime.timedelta(days=args.days - 1)
    iso_filed = [r["filed"] for r in records if re.fullmatch(r"\d{4}-\d{2}-\d{2}", r.get("filed") or "")]
    range_from = min(iso_filed) if iso_filed else fetch_start.isoformat()
    range_to = max(iso_filed) if iso_filed else today.isoformat()

    payload = {
        "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "Duval County Clerk + Property Appraiser + Tax Deed Auction",
        "date_range": {"from": range_from, "to": range_to},
        "total": len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records": records,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=None)
    print(f"\nWrote {len(records)} records to {args.out}")

    if conn is not None:
        db.export_backup_json(conn, args.backup)
        print(f"[db] Wrote full backup to {args.backup}")
        n_csv = db.export_csv(conn, args.csv)
        print(f"[db] Wrote {n_csv} rows to {args.csv}")
        n_hs = db.export_hubspot_csv(conn, args.hubspot_csv)
        print(f"[db] Wrote {n_hs} HubSpot-approved rows to {args.hubspot_csv}")
        conn.close()


if __name__ == "__main__":
    main()
