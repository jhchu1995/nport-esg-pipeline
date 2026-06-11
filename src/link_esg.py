"""
N-PORT holdings -> CIQ companyid -> S&P institutionid -> ESG score + industry
==============================================================================

Links the holdings CSV produced by scrape_nport.py to S&P Global ESG scores
via WRDS, in five steps:

  1. Load N-PORT holdings, filter asset categories, build normalized
     CUSIP/ISIN keys (with US-ISIN -> CUSIP backfill)
  2. Pull the Capital IQ CUSIP/ISIN crosswalk -> map holdings to companyid
  3. Bridge companyid -> institutionid via trucost_common.wrds_companies
     (these are DIFFERENT identifier spaces — the bridge is required)
  4. Pull from sp_esg.wrds_esg (by institutionid): the overall S&P Global
     ESG Score plus CSA industry / industry group / classification /
     sector and industry rank
  5. Merge onto holdings, save, and print a coverage report at every join
     so attrition is measured rather than hidden

Identifier-mapping notes
------------------------
- Issuer-level matching uses the first 6 characters of the CUSIP (the issuer
  prefix), since ESG scores attach to issuers while holdings carry
  instrument-level CUSIPs.
- A US ISIN is 'US' + 9-char CUSIP + check digit, so missing CUSIPs are
  recovered from US ISINs before matching.
- Lookups use plain Python dicts keyed on normalized (UPPER/stripped)
  strings. Mapping through pandas nullable-string dtypes can silently match
  nothing; the dict approach makes key alignment explicit.

Requires a WRDS account with access to: ciq (Capital IQ), sp_esg
(S&P Global ESG), and trucost_common. On first run, the wrds package
prompts for credentials and can create a .pgpass file for future sessions.

Usage
-----
    python src/link_esg.py --year 2024
    python src/link_esg.py --year 2024 --asset-cats EC EP   # equity-only
"""

import argparse
import os

import pandas as pd
import wrds

OVERALL_SCORETYPE = "S&P Global ESG Score"

INDUSTRY_COLS = [
    "csaindustrymapid",              # CSA Industry Map ID
    "csaindustryname",               # CSA Industry Name
    "csaindustrygroupname",          # CSA Industry Group Name
    "csaindustryclassificationname", # CSA Industry Classification Name
    "csasectorname",                 # CSA Sector Name
    "industryrank",                  # Industry Rank
]


def chunks(lst, n=1000):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main():
    ap = argparse.ArgumentParser(description="Link N-PORT holdings to S&P Global ESG scores via WRDS.")
    ap.add_argument("--year", type=int, required=True, help="Holdings/assessment year")
    ap.add_argument("--data-dir", default="data", help="Directory with holdings CSV (default: data/)")
    ap.add_argument("--asset-cats", nargs="+", default=["EC", "EP", "DBT"],
                    help="N-PORT asset categories to keep (default: EC EP DBT)")
    args = ap.parse_args()

    holdings_csv = os.path.join(args.data_dir, f"nport_holdings_{args.year}.csv")
    out_csv      = os.path.join(args.data_dir, f"nport_holdings_{args.year}_esg.csv")

    db = wrds.Connection()   # prompts for WRDS username/password on first run

    # ── 1. Holdings: filter + normalized issuer keys ─────────────────────────
    h = pd.read_csv(holdings_csv, dtype={"cusip": "string", "isin": "string"}, low_memory=False)
    sub = h[h["asset_category"].isin(args.asset_cats)].copy()

    # US ISIN -> CUSIP backfill (US ISIN = 'US' + 9-char CUSIP + check digit)
    mask = sub["cusip"].isna() & sub["isin"].str.startswith("US").fillna(False)
    sub.loc[mask, "cusip"] = sub.loc[mask, "isin"].str[2:11]

    sub["cusip"]  = sub["cusip"].astype("string").str.upper().str.strip()
    sub["isin"]   = sub["isin"].astype("string").str.upper().str.strip()
    sub["cusip6"] = sub["cusip"].str[:6]

    # ── 2. CIQ crosswalk: CUSIP/ISIN -> companyid ────────────────────────────
    print("Pulling CUSIP/ISIN crosswalk from ciq.wrds_ciqsymbol ...")
    xwalk = db.raw_sql("""
        select companyid,
               upper(trim(symboltypecat)) as cat,
               upper(trim(symbolvalue))   as val
        from ciq.wrds_ciqsymbol
        where upper(trim(symboltypecat)) in ('CUSIP', 'ISIN')
          and symbolvalue is not null
    """)
    print(f"  crosswalk rows pulled: {len(xwalk):,}")
    xwalk["companyid"] = pd.to_numeric(xwalk["companyid"], errors="coerce").astype("Int64")

    cus = xwalk[xwalk["cat"] == "CUSIP"].dropna(subset=["val", "companyid"])
    isn = xwalk[xwalk["cat"] == "ISIN"].dropna(subset=["val", "companyid"])

    cusip6_to_cid = dict(zip(cus["val"].str[:6], cus["companyid"]))
    isin_to_cid   = dict(zip(isn["val"],         isn["companyid"]))
    print(f"  unique cusip6 keys: {len(cusip6_to_cid):,} | unique isin keys: {len(isin_to_cid):,}")

    # map holdings -> companyid (CUSIP first, ISIN fills the rest)
    cid = sub["cusip6"].map(cusip6_to_cid)
    cid = cid.fillna(sub["isin"].map(isin_to_cid))
    sub["companyid"] = pd.to_numeric(cid, errors="coerce").astype("Int64")
    nmap = sub["companyid"].notna().sum()
    print(f"Holdings mapped to a CIQ companyid: {nmap:,} / {len(sub):,} ({nmap/len(sub):.1%})")

    # ── 3. Bridge: companyid -> institutionid ────────────────────────────────
    print("\nPulling companyid -> institutionid bridge from trucost_common.wrds_companies ...")
    bridge = db.raw_sql("""
        select distinct companyid, institutionid
        from trucost_common.wrds_companies
        where companyid is not null and institutionid is not null
    """)
    bridge["companyid"]     = pd.to_numeric(bridge["companyid"], errors="coerce").astype("Int64")
    bridge["institutionid"] = pd.to_numeric(bridge["institutionid"], errors="coerce").astype("Int64")
    bridge = bridge.dropna().drop_duplicates("companyid")
    cid_to_inst = dict(zip(bridge["companyid"].astype(int), bridge["institutionid"].astype(int)))
    print(f"  bridge pairs: {len(cid_to_inst):,}")

    sub["institutionid"] = sub["companyid"].map(cid_to_inst).astype("Int64")
    nb = sub["institutionid"].notna().sum()
    print(f"Holdings carrying an institutionid: {nb:,} / {len(sub):,} ({nb/len(sub):.1%})")

    # ── 4. Pull ESG score + industry/sector info by institutionid ───────────
    insts = sub["institutionid"].dropna().astype(int).unique().tolist()
    ov = OVERALL_SCORETYPE.replace("'", "''")
    ind_select = ", ".join(INDUSTRY_COLS)

    frames = []
    for batch in chunks(insts):
        idlist = ",".join(str(x) for x in batch)
        frames.append(db.raw_sql(f"""
            select institutionid, scoredate, scorevalue, {ind_select}
            from sp_esg.wrds_esg
            where assessmentyear = {args.year}
              and scoretype = '{ov}'
              and institutionid in ({idlist})
        """))
    esg = (pd.concat([f for f in frames if len(f)], ignore_index=True) if frames
           else pd.DataFrame(columns=["institutionid", "scoredate", "scorevalue"] + INDUSTRY_COLS))
    print(f"\nESG score rows pulled: {len(esg):,}")

    if len(esg):
        esg = (esg.sort_values("scoredate")
                  .drop_duplicates("institutionid", keep="last")   # latest score per firm-year
                  .rename(columns={"scorevalue": "esg_score"}))
        esg["institutionid"] = pd.to_numeric(esg["institutionid"], errors="coerce").astype("Int64")

    # ── 5. Attach + save + coverage report ───────────────────────────────────
    keep_cols = ["institutionid", "esg_score"] + INDUSTRY_COLS
    scored = (sub.merge(esg[keep_cols], on="institutionid", how="left")
              if len(esg) else sub.assign(esg_score=pd.NA, **{c: pd.NA for c in INDUSTRY_COLS}))
    scored.to_csv(out_csv, index=False)

    ec = scored[scored["asset_category"] == "EC"]
    print(f"\nSaved -> {out_csv}")
    if len(ec):
        print(f"Equity (EC) rows with an institutionid : {ec['institutionid'].notna().mean():.1%}")
        print(f"Equity (EC) rows with an ESG score     : {ec['esg_score'].notna().mean():.1%}")
        print(f"Equity (EC) rows with a CSA industry   : {ec['csaindustryname'].notna().mean():.1%}")
        print("\nSector distribution of scored equity holdings:")
        print(ec["csasectorname"].value_counts(dropna=False).head(15).to_string())


if __name__ == "__main__":
    main()
