"""
N-PORT holdings scraper
=======================

Enumerates every NPORT-P filing for a calendar year from SEC EDGAR's quarterly
master index, downloads and caches the raw XML, and parses holdings into a CSV.

Design notes
------------
1. FILER ENUMERATION uses EDGAR's quarterly MASTER INDEX
   (/Archives/edgar/full-index/YYYY/QTRn/master.idx) instead of full-text
   search. Full-text search (efts.sec.gov) requires a keyword and hard-caps
   results at 10,000, so it can never enumerate an entire form type. The
   master index lists every filing of every form type for a quarter — four
   downloads cover the year, with no cap.

2. RAW XML IS CACHED to data/raw_xml/<accession>.xml. If a parsing bug turns
   up later, the cache is re-parsed in minutes instead of re-downloading a
   multi-GB corpus.

3. IDENTIFIER PARSING reads identifiers from where the N-PORT schema actually
   puts them: CUSIP is a direct child of <invstOrSec>, but ISIN / ticker /
   other live inside an <identifiers> sub-element as *attributes*
   (<isin value="..."/>). Placeholder values ("N/A", "000000000", ...) are
   treated as missing, and a single resolved `primary_id` is built using
   SEC's own priority: cusip > isin > ticker > other.

4. RESUMABLE + INCREMENTAL. Holdings are appended to the output CSV as they
   are parsed and each finished accession is logged. Re-running skips what's
   already done, so a crash at filing 30,000 doesn't cost the first 30,000.

Fair access: a full year of NPORT-P is tens of thousands of filings. At the
SEC's 10 req/sec limit this is a multi-hour, multi-GB job. Test with
--max-filings 200 first.

Usage
-----
    export SEC_USER_AGENT="Your Name you@example.com"   # SEC requires name + email
    python src/scrape_nport.py --year 2024 --max-filings 200   # test slice
    python src/scrape_nport.py --year 2024                     # full year
"""

import argparse
import csv
import os
import re
import time

import pandas as pd
import requests
from lxml import etree
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────
FORM_TYPES = {"NPORT-P"}        # add "NPORT-P/A" to also collect amendments

RATE_DELAY       = 0.12         # seconds between requests (~8/sec, under SEC's 10/sec)
TIMEOUT          = 20
MAX_RETRIES      = 4
RETRY_WAIT       = 5
CHECKPOINT_EVERY = 500          # progress print cadence

PLACEHOLDER_IDS = {"", "N/A", "NA", "NONE", "0", "00000000", "000000000", "000000000000"}

SESSION = requests.Session()


# ── Safe request wrapper: timeout + retry + rate limit ───────────────────────
def safe_get(url, retries=MAX_RETRIES, stream=False):
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=TIMEOUT, stream=stream)
            time.sleep(RATE_DELAY)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None                      # genuinely missing; don't retry
            print(f"\n  HTTP {resp.status_code} (attempt {attempt+1}/{retries}): {url[:90]}")
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"\n  {type(e).__name__} (attempt {attempt+1}/{retries}): {url[:90]}")
        time.sleep(RETRY_WAIT)
    return None


def _clean(val):
    """Return None for placeholder / empty identifier values."""
    if val is None:
        return None
    v = val.strip()
    return None if v.upper() in PLACEHOLDER_IDS else v


def valid_cusip(c):
    """Modulus-10 check-digit validation; flags truncated/garbage CUSIPs."""
    if not c or len(c) != 9:
        return False
    total = 0
    for i, ch in enumerate(c[:8]):
        if ch.isdigit():
            v = int(ch)
        elif ch.isalpha():
            v = ord(ch.upper()) - 55             # A=10 ... Z=35
        elif ch == "*":
            v = 36
        elif ch == "@":
            v = 37
        elif ch == "#":
            v = 38
        else:
            return False
        if i % 2 == 1:
            v *= 2
        total += v // 10 + v % 10
    return str((10 - total % 10) % 10) == c[8]


# ── Step 1: Enumerate NPORT-P filings from the quarterly master index ─────────
def get_nport_filings(year):
    print(f"Enumerating {sorted(FORM_TYPES)} filings for {year} from EDGAR master index...")
    rows = []
    for qtr in (1, 2, 3, 4):
        url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{qtr}/master.idx"
        resp = safe_get(url)
        if resp is None:
            print(f"  Could not fetch QTR{qtr} index.")
            continue

        text = resp.content.decode("latin-1")
        # Data begins after the dashed separator line.
        if "---" in text:
            text = text.split("---", 1)[1].split("\n", 1)[1]

        for line in text.splitlines():
            parts = line.split("|")
            if len(parts) != 5:
                continue
            cik, company, form, date_filed, filename = parts
            if form not in FORM_TYPES:
                continue
            # filename: edgar/data/<cik>/<accession>.txt
            accession = os.path.basename(filename).replace(".txt", "")
            rows.append({
                "cik": cik.strip(),
                "company": company.strip(),
                "form": form.strip(),
                "date_filed": date_filed.strip(),
                "accession": accession.strip(),
            })
        print(f"  QTR{qtr}: running total {len(rows):,} matching filings")

    df = pd.DataFrame(rows).drop_duplicates(subset=["accession"]).reset_index(drop=True)
    print(f"Total {sorted(FORM_TYPES)} filings in {year}: {len(df):,}")
    return df


# ── Step 2: Locate + download the raw N-PORT XML (cached) ────────────────────
def get_xml_path(cik, accession, raw_xml_dir):
    """Return local path to the raw primary_doc.xml, downloading + caching if needed."""
    os.makedirs(raw_xml_dir, exist_ok=True)
    local = os.path.join(raw_xml_dir, f"{accession}.xml")
    if os.path.exists(local) and os.path.getsize(local) > 0:
        return local

    acc_nodash = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}"

    # Primary doc for NPORT-P is reliably named primary_doc.xml; try it directly.
    resp = safe_get(f"{base}/primary_doc.xml")

    # Fallback: read the filing's index.json and find the real XML file.
    if resp is None:
        idx = safe_get(f"{base}/index.json")
        if idx is not None:
            try:
                items = idx.json().get("directory", {}).get("item", [])
                xml_names = [it["name"] for it in items
                             if it.get("name", "").lower().endswith(".xml")
                             and "xsl" not in it.get("name", "").lower()]
                if xml_names:
                    resp = safe_get(f"{base}/{xml_names[0]}")
            except Exception:
                resp = None

    if resp is None:
        return None

    with open(local, "wb") as f:
        f.write(resp.content)
    return local


# ── Step 3: Parse a raw N-PORT XML file into a holdings DataFrame ─────────────
def parse_nport_xml(path, cik, accession, company):
    try:
        tree = etree.parse(path)
        root = tree.getroot()
    except Exception as e:
        print(f"  Parse error {accession}: {e}")
        return pd.DataFrame()

    # N-PORT uses a versioned namespace; detect it from the root tag.
    m = re.match(r"\{(.+?)\}", root.tag)
    ns = {"n": m.group(1) if m else "http://www.sec.gov/edgar/nport"}

    def find_text(parent, path_):
        el = parent.find(path_, ns) if parent is not None else None
        return el.text.strip() if el is not None and el.text else ""

    gen_info  = root.find(".//n:genInfo", ns)
    fund_info = root.find(".//n:fundInfo", ns)
    series_name = find_text(gen_info, "n:seriesName")
    series_id   = find_text(gen_info, "n:seriesId")
    reg_name    = find_text(gen_info, "n:regName")
    net_assets  = find_text(fund_info, "n:netAssets")
    period      = find_text(gen_info, "n:repPdDate") or find_text(gen_info, "n:repPdEnd")

    holdings = []
    for inv in root.findall(".//n:invstOrSecs/n:invstOrSec", ns):

        def text(tag):
            el = inv.find(f"n:{tag}", ns)
            return el.text.strip() if el is not None and el.text else ""

        def ident(tag):   # isin / ticker / other are @value inside <identifiers>
            el = inv.find(f"n:identifiers/n:{tag}", ns)
            return el.get("value") if el is not None else None

        cusip  = _clean(text("cusip"))
        isin   = _clean(ident("isin"))
        ticker = _clean(ident("ticker"))
        other_el   = inv.find("n:identifiers/n:other", ns)
        other_val  = _clean(other_el.get("value")) if other_el is not None else None
        other_desc = other_el.get("otherDesc") if other_el is not None else None

        primary_id = cusip or isin or ticker or other_val
        primary_id_type = ("cusip" if cusip else "isin" if isin else
                           "ticker" if ticker else "other" if other_val else "none")

        holdings.append({
            "cik"             : cik,
            "accession"       : accession,
            "registrant_name" : reg_name or company,
            "series_name"     : series_name,
            "series_id"       : series_id,
            "period"          : period,
            "fund_net_assets" : net_assets,
            "security_name"   : text("name"),
            "title"           : text("title"),
            "lei"             : _clean(text("lei")),
            "cusip"           : cusip,
            "cusip_valid"     : valid_cusip(cusip) if cusip else False,
            "isin"            : isin,
            "ticker"          : ticker,
            "other_id"        : other_val,
            "other_id_desc"   : other_desc,
            "primary_id"      : primary_id,
            "primary_id_type" : primary_id_type,
            "balance"         : text("balance"),
            "units"           : text("units"),
            "currency"        : text("curCd"),
            "value_usd"       : text("valUSD"),
            "pct_nav"         : text("pctVal"),
            "asset_category"  : text("assetCat"),
            "issuer_category" : text("issuerCat"),
            "country"         : text("invCountry"),
            "is_restricted"   : text("isRestrictedSec"),
        })

    return pd.DataFrame(holdings)


# ── Step 4: Main pipeline (resumable, incremental writes) ─────────────────────
def main():
    ap = argparse.ArgumentParser(description="Scrape N-PORT-P holdings from SEC EDGAR.")
    ap.add_argument("--year", type=int, required=True, help="Calendar year to scrape")
    ap.add_argument("--max-filings", type=int, default=None,
                    help="Limit number of filings (use ~200 to test before a full run)")
    ap.add_argument("--data-dir", default="data", help="Output directory (default: data/)")
    args = ap.parse_args()

    user_agent = os.environ.get("SEC_USER_AGENT", "")
    if not user_agent or "@" not in user_agent:
        raise SystemExit(
            'Set the SEC_USER_AGENT environment variable to your real name and email,\n'
            'e.g.  export SEC_USER_AGENT="Jane Doe jane@university.edu"\n'
            "The SEC requires this for fair-access identification."
        )
    SESSION.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})

    data_dir      = args.data_dir
    raw_xml_dir   = os.path.join(data_dir, "raw_xml")
    filers_file   = os.path.join(data_dir, f"nport_filers_{args.year}.csv")
    output_file   = os.path.join(data_dir, f"nport_holdings_{args.year}.csv")
    processed_log = os.path.join(data_dir, f"processed_{args.year}.log")
    os.makedirs(data_dir, exist_ok=True)

    filers = get_nport_filings(args.year)
    filers.to_csv(filers_file, index=False)
    print(f"Saved filer list -> {filers_file}")

    if args.max_filings:
        filers = filers.head(args.max_filings)

    # Resume: skip accessions already completed.
    done = set()
    if os.path.exists(processed_log):
        with open(processed_log) as f:
            done = {ln.strip() for ln in f if ln.strip()}
        print(f"Resuming: {len(done):,} filings already processed, skipping them.")

    output_exists = os.path.exists(output_file)
    type_counts = {"cusip": 0, "isin": 0, "ticker": 0, "other": 0, "none": 0}
    failed, total_rows = [], 0

    out_f = open(output_file, "a", newline="", encoding="utf-8")
    log_f = open(processed_log, "a", encoding="utf-8")
    writer = None

    try:
        for i, row in enumerate(tqdm(filers.itertuples(index=False),
                                     total=len(filers), desc="Filings")):
            if row.accession in done:
                continue

            path = get_xml_path(row.cik, row.accession, raw_xml_dir)
            if path is None:
                failed.append(row.accession)
                log_f.write(row.accession + "\n"); log_f.flush()
                continue

            df = parse_nport_xml(path, row.cik, row.accession, row.company)
            if not df.empty:
                if writer is None:
                    writer = csv.DictWriter(out_f, fieldnames=list(df.columns))
                    if not output_exists:
                        writer.writeheader()
                for rec in df.to_dict("records"):
                    writer.writerow(rec)
                out_f.flush()
                total_rows += len(df)
                for t in df["primary_id_type"]:
                    type_counts[t] = type_counts.get(t, 0) + 1

            log_f.write(row.accession + "\n"); log_f.flush()

            if (i + 1) % CHECKPOINT_EVERY == 0:
                print(f"\n  ...{i+1:,} filings, {total_rows:,} holdings written so far")
    finally:
        out_f.close()
        log_f.close()

    # ── Coverage report ──────────────────────────────────────────────────────
    print(f"\nDone. Holdings written -> {output_file}")
    print(f"  Total holding rows this run : {total_rows:,}")
    print(f"  Failed filings (no XML)     : {len(failed):,}")
    if total_rows:
        print("  Identifier coverage (resolved primary_id):")
        for t in ("cusip", "isin", "ticker", "other", "none"):
            c = type_counts.get(t, 0)
            print(f"    {t:7s}: {c:>9,}  ({100*c/total_rows:5.1f}%)")
        print("  Note: rows with primary_id_type == 'none' are truly unidentified")
        print("        in the source filing, not a parsing failure.")


if __name__ == "__main__":
    main()
