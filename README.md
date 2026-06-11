# Measuring ESG Exposure of U.S. Mutual Fund Portfolios

**A reproducible pipeline linking SEC N-PORT holdings to S&P Global ESG scores and CSA industry/sector classifications**

This repository builds, from primary sources, a fund-holding-level dataset of ESG exposure for the U.S. registered fund universe. It scrapes every N-PORT-P filing from SEC EDGAR for a calendar year (~tens of thousands of filings, tens of millions of holding rows), resolves each holding to an issuer through a chain of financial identifier systems, and enriches each holding with two layers of issuer-level data via WRDS: the S&P Global ESG Score, and the full S&P/CSA industry taxonomy (sector, industry group, industry, classification, and the issuer's ESG rank within its industry). The industry layer matters as much as the score: raw ESG scores are not comparable across industries — an oil major and a software firm are assessed on different criteria — so within-industry ranks and sector tags are what make cross-portfolio comparisons meaningful.

## Research motivation

Funds increasingly market themselves on sustainability, but a fund's *actual* ESG exposure is only observable from its disclosed holdings. Form N-PORT — the SEC's monthly portfolio disclosure for registered investment companies — makes those holdings public, but in a form that is hard to use at scale: the data live in tens of thousands of separate XML filings, and securities are identified by instrument-level codes (CUSIP/ISIN) while ESG scores are assigned at the *issuer* level under entirely different identifier systems. Bridging that gap is the core problem this pipeline solves, and the resulting dataset supports questions like:

- How does measured portfolio ESG exposure compare to funds' stated mandates ("greenwashing" detection)?
- How is ESG exposure distributed across fund families, asset classes, and sectors?
- Do "sustainable" funds achieve their ESG profile by holding genuinely best-in-class issuers (high industry rank), or simply by tilting toward structurally high-scoring sectors?
- How do portfolio ESG profiles shift around scoring events or regulatory changes?

## Pipeline overview

```
SEC EDGAR master index ──> N-PORT-P XML filings ──> holdings (CUSIP/ISIN/ticker/LEI)
                                                          │
                                   CIQ identifier crosswalk (ciq.wrds_ciqsymbol)
                                                          │
                                                   CIQ companyid
                                                          │
                              issuer bridge (trucost_common.wrds_companies)
                                                          │
                                                S&P institutionid
                                                          │
                        ESG scores + CSA industry/sector (sp_esg.wrds_esg)
```

**Stage 1 — `src/scrape_nport.py`** (no subscriptions required, only an internet connection)

1. Enumerates all N-PORT-P filings for a year from EDGAR's **quarterly master index** rather than full-text search. Full-text search requires a keyword and caps results at 10,000, so it can never enumerate a full form type; the master index lists every filing with no cap.
2. Downloads and **caches the raw XML** per filing, so parsing bugs discovered later can be fixed by re-parsing locally instead of re-downloading a multi-GB corpus.
3. Parses each filing's holdings, reading identifiers from where they actually live in the schema: CUSIP is a direct child of `<invstOrSec>`, but ISIN/ticker/other are *attributes* of elements inside an `<identifiers>` block — an easy detail to miss that silently destroys identifier coverage.
4. Validates CUSIPs with the modulus-10 check digit, treats placeholder values (`N/A`, `000000000`, ...) as missing, and resolves a single `primary_id` per holding using SEC's own priority order (CUSIP > ISIN > ticker > other).
5. Writes **incrementally and resumably**: holdings are appended as they are parsed and each completed accession is logged, so a crash at filing 30,000 costs nothing.

**Stage 2 — `src/link_esg.py`** (requires a WRDS account with S&P Global ESG and Capital IQ access)

1. Backfills missing CUSIPs from US ISINs (a US ISIN is `US` + 9-char CUSIP + check digit).
2. Maps holdings to **CIQ `companyid`** via the Capital IQ symbol crosswalk, matching on the 6-character issuer prefix of the CUSIP (instrument → issuer aggregation) with ISIN as fallback.
3. Bridges `companyid` to **S&P `institutionid`** — these are *different identifier spaces*, a fact the pipeline verifies with an explicit spot check rather than assuming.
4. Pulls the latest S&P Global ESG Score per issuer-year plus CSA industry, industry group, sector, and industry rank, and merges them onto holdings.
5. Prints a coverage report at every join so attrition is measured, not hidden.

## Identifier mapping: the hard part

The intellectual core of this project is that no two datasets here speak the same identifier language:

| System | Level | Lives in |
|---|---|---|
| CUSIP / ISIN / ticker / LEI | security (instrument) | N-PORT filings |
| CIQ `companyid` | issuer | Capital IQ crosswalk |
| S&P `institutionid` | issuer | S&P Global ESG |

Non-obvious issues this pipeline handles explicitly: instrument-vs-issuer aggregation via CUSIP6; US-ISIN-to-CUSIP recovery; placeholder/garbage identifiers; check-digit validation; dtype pitfalls in pandas string joins (normalized plain-string dict lookups instead of nullable-string `.map()`, which can silently match nothing); and the `companyid` ≠ `institutionid` distinction, bridged through `trucost_common.wrds_companies`.

## Usage

```bash
pip install -r requirements.txt

# Stage 1: scrape (SEC requires a real name + email in the User-Agent)
export SEC_USER_AGENT="Jane Doe jane@university.edu"
python src/scrape_nport.py --year 2024 --max-filings 200   # test slice first
python src/scrape_nport.py --year 2024                     # full year (multi-hour, multi-GB)

# Stage 2: link to ESG scores (prompts for WRDS credentials)
python src/link_esg.py --year 2024
```

Outputs land in `data/` (gitignored): the filer list, the raw XML cache, the holdings CSV, and the final enriched CSV.

## Output dataset

The final file (`nport_holdings_<year>_esg.csv`) is one row per holding, carrying three layers of information:

| Layer | Columns (selection) |
|---|---|
| **Holding** (from N-PORT) | fund/series identity, period, security name, balance, value (USD), % of NAV, asset & issuer category, country, all raw identifiers (CUSIP/ISIN/ticker/LEI) |
| **Issuer resolution** | resolved `primary_id` + type, CIQ `companyid`, S&P `institutionid` |
| **ESG & industry** (from S&P Global) | `esg_score`, `csasectorname`, `csaindustrygroupname`, `csaindustryname`, `csaindustryclassificationname`, `csaindustrymapid`, `industryrank` |

Because ESG scores and industry tags attach at the issuer level, every holding of the same issuer (common stock, preferred, bonds) inherits the same classification — which is exactly what's needed for portfolio-level aggregation by sector or industry-relative quality.

## Responsible use & data licensing

- The scraper stays under the SEC's 10 requests/second fair-access limit and identifies itself as the SEC requires. A full year is a multi-hour job by design; please keep it that way.
- **No S&P Global / WRDS data is redistributed in this repository.** ESG scores and the Capital IQ crosswalk are licensed datasets; the code reproduces the build for users with their own WRDS access.
- N-PORT filings are public records, but the raw XML cache and derived CSVs are excluded from version control due to size.

## Repository structure

```
├── src/
│   ├── scrape_nport.py   # Stage 1: EDGAR → holdings CSV
│   └── link_esg.py       # Stage 2: holdings → ESG scores + CSA industry/sector
├── data/                 # outputs (gitignored)
├── requirements.txt
└── README.md
```
