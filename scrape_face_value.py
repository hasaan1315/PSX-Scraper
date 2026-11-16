"""
Scrape face value (and share count) from each PSX company page.

PSX shows both under "Equity Profile" on https://dps.psx.com.pk/company/<TICKER>.
Dividends are quoted as % of face value, so Rs per share = pct / 100 * face_value.

Tickers: every ticker found in bronze/dividends/*.csv (union of all files).
         If none exist, falls back to all non-debt tickers from PSX.

Output : bronze/face_value/face_value_YYYYMMDD.csv
         columns: ticker, face_value, shares_outstanding, ingestion_date

Run    : python scrape_face_value.py            (all dividend tickers)
         python scrape_face_value.py --tickers AHCL,DLL,HBL   (quick test)
"""
import argparse
import glob
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psxdata
from bs4 import BeautifulSoup
from psxdata.scrapers.base import BaseScraper

SLEEP = 0.3
_scraper = BaseScraper()

SECTION = re.compile(r"Equity Profile(.*?)Announcements", re.S)
FACE = re.compile(r"Face Value\s+([\d.,]+)")
SHARES = re.compile(r"Shares\s+([\d,]+)")


def parse_equity_profile(text):
    """Return (face_value, shares) from page text. None when not found."""
    m = SECTION.search(text)
    if not m:
        return None, None
    block = m.group(1)
    f = FACE.search(block)
    s = SHARES.search(block)
    face = float(f.group(1).replace(",", "")) if f else None
    shares = int(s.group(1).replace(",", "")) if s else None
    return face, shares


def get_face_value(symbol):
    r = _scraper._request("GET", f"https://dps.psx.com.pk/company/{symbol}")
    text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
    return parse_equity_profile(text)


def tickers_from_bronze():
    found = set()
    for path in glob.glob("bronze/dividends/*.csv"):
        df = pd.read_csv(path, usecols=["ticker"])
        found.update(df["ticker"].dropna().astype(str))
    return sorted(found)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", help="comma list, e.g. AHCL,DLL,HBL")
    args = ap.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = tickers_from_bronze()
        if not tickers:
            syms = psxdata.symbols()
            tickers = syms.loc[~syms["is_debt"].astype(bool), "symbol"].tolist()
    print(f"{len(tickers)} tickers")

    rows, failed = [], []
    for i, t in enumerate(tickers, 1):
        try:
            face, shares = get_face_value(t)
            print(f"[{i}/{len(tickers)}] {t}: face={face} shares={shares}")
            if face is None:
                failed.append(t)
            else:
                rows.append({"ticker": t, "face_value": face, "shares_outstanding": shares})
        except Exception as e:
            print(f"[{i}/{len(tickers)}] {t}: error {e}")
            failed.append(t)
        time.sleep(SLEEP)

    out = Path("bronze/face_value")
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")
    df = pd.DataFrame(rows, columns=["ticker", "face_value", "shares_outstanding"])
    df["ingestion_date"] = datetime.now().strftime("%Y-%m-%d")
    path = out / f"face_value_{stamp}.csv"
    df.to_csv(path, index=False)
    print(f"\nSaved {len(df)} rows -> {path}")
    print(f"{len(failed)} tickers without a face value: {failed[:30]}")
    print(df["face_value"].value_counts().to_string())


if __name__ == "__main__":
    main()
