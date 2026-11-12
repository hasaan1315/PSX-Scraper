import pandas as pd, glob
import scraper as p

path = glob.glob("bronze/dividends/dividends_*.csv")[-1]
d = pd.read_csv(path)
d = d.drop_duplicates(
    subset=["ticker", "announcement_ts", "period", "details_raw", "closure_raw"]
)

rows = []
for _, r in d.iterrows():
    for it in p.parse_details(r["details_raw"]):
        x = r.to_dict()
        x.update(payout_pct=it["pct"], payout_tag=it["tag"],
                 payout_type=it["type"], right_price=it["right_price"],
                 right_kind=it["right_kind"], parse_status=it["parse_status"])
        rows.append(x)

fixed = pd.DataFrame(rows)
fixed.to_csv(path.replace(".csv", "_reparsed.csv"), index=False)
print(fixed["payout_type"].value_counts(dropna=False))

raw = pd.read_csv(path)
print(raw.duplicated(subset=["ticker", "announcement_ts", "period",
                             "details_raw", "closure_raw"]).sum())

print(fixed.duplicated(subset=["ticker", "announcement_ts", "period",
                               "payout_type", "payout_pct", "payout_tag"]).sum(), "true duplicates")
print(fixed["parse_status"].value_counts())

dup = fixed[fixed.duplicated(subset=["ticker", "announcement_ts", "period",
                                     "payout_type", "payout_pct", "payout_tag"], keep=False)]
print(dup[["ticker", "announcement_ts", "period", "payout_pct",
           "details_raw", "closure_raw"]].to_string())