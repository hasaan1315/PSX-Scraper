"""
PSX Bronze ingestion: daily prices + dividend announcements.

Source : PSX (dps.psx.com.pk) via psxdata (handles X-Req-Id token).
Output : append-only dated CSV files (Bronze layer).
           bronze/prices/prices_YYYYMMDD.csv
           bronze/dividends/dividends_YYYYMMDD.csv
           bronze/failed/failed_prices_YYYYMMDD.csv
           bronze/failed/failed_dividends_YYYYMMDD.csv

Install : pip install psxdata beautifulsoup4 pandas
Run     : python psx_bronze.py                 (prices 30 days + dividends)
          python psx_bronze.py --days 5        (prices last 5 days)
          python psx_bronze.py --only prices   (or: dividends)
          python psx_bronze.py --limit 10      (test on first 10 tickers)
"""
import argparse
import re
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import psxdata
from bs4 import BeautifulSoup
from psxdata.scrapers.base import BaseScraper

warnings.filterwarnings("ignore")

PAYOUTS_URL = "https://dps.psx.com.pk/company/payouts"
SLEEP = 0.5

# Clean form: "60%(iii) (D)", "42.50%(ii) (D)", "80(ii) (D)", "250%(F) (D)"
ITEM = re.compile(r"([\d.]+)\s*%?\s*(?:\(([^)]*)\))?\s*\(([A-Z])\)")
TAG = re.compile(r"%\s*[()]*\s*(I{1,3}|IV|V|F)\b")
RIGHT_PRICE = re.compile(r"RS\.?\s*(\d+(?:\.\d+)?)")


def parse_details(text):
    """Parse a PSX 'Details' cell into 0..n payout dicts.

    Handles clean text, typos ("12.5%(ii (D)", "45%F) (D)", "360%i) (D)"),
    "DIVIDEND =15%(i)" (no type letter) and right shares
    ("100% AT A DISCOUNT OF RS. 5/= PER SHARE (R)").
    parse_status: clean | inferred | fallback | unparsed
    """
    t = (text or "").strip()
    out = []

    for pct, tag, typ in ITEM.findall(t):
        status = "clean"
        if typ == "F":              # "50%(F)" with no (D): final dividend
            tag, typ, status = "F", "D", "inferred"
        out.append({"pct": float(pct), "tag": tag.strip(), "type": typ,
                    "right_price": None, "right_kind": None,
                    "parse_status": status})
        if out:
         seen, uniq = set(), []      # PSX sometimes repeats an item in one cell
         for o in out:
            k = (o["pct"], o["tag"], o["type"])
            if k not in seen:
                seen.add(k)
                uniq.append(o)
        return uniq

    u = t.upper()
    m = re.search(r"(\d+(?:\.\d+)?)", u)
    if not m:
        return [{"pct": None, "tag": "", "type": None, "right_price": None,
                 "right_kind": None, "parse_status": "unparsed"}]

    tag_m = TAG.search(u)
    tag = tag_m.group(1) if tag_m else ""
    tag = tag if tag == "F" else tag.lower()

    type_m = re.search(r"\(([DBR])\)", u)
    right_kind = ("premium" if "PREMIUM" in u
                  else "discount" if "DISCOUNT" in u else None)
    if type_m:
        typ = type_m.group(1)
    elif right_kind:
        typ = "R"
    elif "DIVIDEND" in u or tag:
        typ = "D"
    else:
        typ = None

    price_m = RIGHT_PRICE.search(u) if typ == "R" else None
    return [{"pct": float(m.group(1)), "tag": tag, "type": typ,
             "right_price": float(price_m.group(1)) if price_m else None,
             "right_kind": right_kind,
             "parse_status": "fallback" if typ else "unparsed"}]
DATE = re.compile(r"\d{2}/\d{2}/\d{4}")

_scraper = None


def scraper():
    """One shared BaseScraper: keeps the X-Req-Id token, retries, rate limit."""
    global _scraper
    if _scraper is None:
        _scraper = BaseScraper()
    return _scraper


# ---------------------------------------------------------------- prices
def get_prices(symbol, start, end):
    df = psxdata.stocks(symbol, start=start, end=end, cache=False)
    if df is None or df.empty:
        return None
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["ticker"] = symbol
    cols = ["date", "ticker", "open", "high", "low", "close", "volume", "is_anomaly"]
    return df[[c for c in cols if c in df.columns]]


# ------------------------------------------------------------- dividends
def get_payouts(symbol):
    r = scraper()._request("POST", PAYOUTS_URL, data={"symbol": symbol})
    soup = BeautifulSoup(r.text, "html.parser")
    rows = []
    for tr in soup.select("tbody tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(tds) < 4:
            continue
        ann, period, details, closure = tds[:4]

        dates = DATE.findall(closure)
        start = pd.to_datetime(dates[0], format="%d/%m/%Y") if dates else pd.NaT
        end = pd.to_datetime(dates[-1], format="%d/%m/%Y") if dates else pd.NaT

        for it in parse_details(details):
            rows.append({
                "ticker": symbol,
                "announcement_ts": pd.to_datetime(ann, errors="coerce"),
                "period": period,
                "payout_pct": it["pct"],
                "payout_tag": it["tag"],
                "payout_type": it["type"],  # D = cash, B = bonus, R = right
                "right_price": it["right_price"],
                "right_kind": it["right_kind"],
                "parse_status": it["parse_status"],
                "details_raw": details,
                "book_closure_start": start,
                "book_closure_end": end,
                "closure_raw": closure,
            })
    return rows


# ---------------------------------------------------------------- runner
def save(df, folder, name):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    df.to_csv(path, index=False)
    return path


def run_prices(tickers, out, stamp, ingestion, days):
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")
    data, failed = [], []

    for i, t in enumerate(tickers, 1):
        print(f"[prices {i}/{len(tickers)}] {t}")
        try:
            df = get_prices(t, start, end)
            if df is None:
                failed.append({"ticker": t, "reason": "no data"})
            else:
                data.append(df)
        except Exception as e:
            print(f"  error {t}: {e}")
            failed.append({"ticker": t, "reason": str(e)[:200]})
        time.sleep(SLEEP)

    if data:
        final = pd.concat(data, ignore_index=True)
        final["ingestion_date"] = ingestion
        p = save(final, out / "prices", f"prices_{stamp}.csv")
        print(f"\nPrices: {len(final)} rows -> {p}")
    else:
        print("\nPrices: no data extracted")

    save(pd.DataFrame(failed, columns=["ticker", "reason"]),
         out / "failed", f"failed_prices_{stamp}.csv")
    print(f"Prices: {len(failed)} tickers with no data or errors")


def run_dividends(tickers, out, stamp, ingestion):
    rows, failed = [], []

    for i, t in enumerate(tickers, 1):
        print(f"[dividends {i}/{len(tickers)}] {t}")
        try:
            rows.extend(get_payouts(t))
        except Exception as e:
            print(f"  error {t}: {e}")
            failed.append({"ticker": t, "reason": str(e)[:200]})
        time.sleep(SLEEP)

    df = pd.DataFrame(rows)
    if df.empty:
        print("\nDividends: no rows found")
    else:
        df["ingestion_date"] = ingestion
        p = save(df, out / "dividends", f"dividends_{stamp}.csv")
        print(f"\nDividends: {len(df)} rows -> {p}")

        # quality checks
        bad = df[df["payout_pct"].isna()]
        print(f"Dividends: {len(bad)} rows with no parsed pct")
        if len(bad):
            print(bad[["ticker", "details_raw"]]
                  .drop_duplicates("details_raw").head(20).to_string(index=False))
        print("Payout types:")
        print(df["payout_type"].value_counts(dropna=False).to_string())
        print("Parse status:")
        print(df["parse_status"].value_counts(dropna=False).to_string())

    save(pd.DataFrame(failed, columns=["ticker", "reason"]),
         out / "failed", f"failed_dividends_{stamp}.csv")
    print(f"Dividends: {len(failed)} tickers with errors")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="price history window")
    ap.add_argument("--only", choices=["prices", "dividends"])
    ap.add_argument("--limit", type=int, help="only first N tickers (testing)")
    ap.add_argument("--out", default="bronze", help="output folder")
    args = ap.parse_args()

    out = Path(args.out)
    stamp = datetime.now().strftime("%Y%m%d")
    ingestion = datetime.now().strftime("%Y-%m-%d")

    tickers = list(psxdata.tickers())
    if args.limit:
        tickers = tickers[: args.limit]
    print(f"{len(tickers)} tickers\n")

    if args.only in (None, "prices"):
        run_prices(tickers, out, stamp, ingestion, args.days)
    if args.only in (None, "dividends"):
        run_dividends(tickers, out, stamp, ingestion)


if __name__ == "__main__":
    main()