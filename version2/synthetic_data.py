import os
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

# -------------------------
# Configuration
# -------------------------
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://main_user:main_user@cluster0.d8jtf.mongodb.net/?retryWrites=true&w=majority")
DB_NAME = os.getenv("DB_NAME", "marketdata")
TS_COLL = os.getenv("TS_COLL", "stock_ticks_v2")
SYMBOL = os.getenv("SYMBOL", "KO")

START_DATE = pd.Timestamp("2020-01-01", tz="UTC")
END_DATE = pd.Timestamp("2025-09-08", tz="UTC")

# Split events (date → factor)
SPLITS = {
    pd.Timestamp("2022-03-21", tz="UTC"): 2,  # 1:2
    pd.Timestamp("2025-01-09", tz="UTC"): 5,  # 1:5
}

# Price simulation parameters
SEED = 42
START_PRICE = 50.0
DAILY_DRIFT = 0.0003
DAILY_VOL = 0.02
BASE_VOL = 4_000_000

# -------------------------
# Helpers
# -------------------------
def business_days(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    return pd.bdate_range(start, end, freq="C", tz="UTC")

def make_underlying_path(dates: pd.DatetimeIndex) -> np.ndarray:
    np.random.seed(SEED)
    n = len(dates)
    rets = np.random.normal(loc=DAILY_DRIFT, scale=DAILY_VOL, size=n)
    path = np.empty(n)
    path[0] = START_PRICE
    for i in range(1, n):
        path[i] = path[i-1] * np.exp(rets[i])
    return path

def cumulative_split_factor(dates: pd.DatetimeIndex) -> np.ndarray:
    factors = np.ones(len(dates))
    cum = 1.0
    split_map = {d: f for d, f in SPLITS.items()}
    for i, d in enumerate(dates):
        if d.normalize() in split_map:
            cum *= split_map[d.normalize()]
        factors[i] = cum
    return factors

# -------------------------
# Build DataFrame
# -------------------------
def build_dataframe():
    dates = business_days(START_DATE, END_DATE)
    und = make_underlying_path(dates)
    cum_factor = cumulative_split_factor(dates)

    price_raw = und / cum_factor
    price_adj = und

    vol_adj = np.random.lognormal(mean=np.log(BASE_VOL), sigma=0.25, size=len(dates)).astype(int)
    vol_raw = (vol_adj * cum_factor).astype(int)

    df = pd.DataFrame({
        "ts": dates,
        "validStart": dates,
        "price_raw": price_raw,
        "price_adjusted": price_adj,
        "volume": vol_raw,
        "eventTags": [[] for _ in range(len(dates))]
    })

    # Event tags
    for split_date, factor in SPLITS.items():
        idx = df.index[df["ts"].dt.normalize() == split_date.normalize()]
        if len(idx) > 0:
            label = f"1:{factor}"
            df.loc[idx, "eventTags"] = df.loc[idx, "eventTags"].apply(lambda _: [{"type": "SPLIT", "factor": int(factor), "label": label}])

    return df

# -------------------------
# MongoDB time-series setup
# -------------------------
def ensure_collections(client: MongoClient):
    db = client[DB_NAME]
    try:
        db.create_collection(
            TS_COLL,
            timeseries={
                "timeField": "ts",
                "metaField": "meta",
                "granularity": "hours",  # recommended for daily/hourly
            }
        )
    except CollectionInvalid:
        pass  # already exists

    # Indexes
    db[TS_COLL].create_index([("meta.symbol", 1), ("ts", 1)], name="symbol_ts")
    return db

# -------------------------
# Main
# -------------------------
def main():
    client = MongoClient(MONGODB_URI)
    db = ensure_collections(client)

    df = build_dataframe()
    docs = []
    for row in df.itertuples(index=False):
        doc = {
            "meta": {"symbol": SYMBOL},
            "ts": row.ts.to_pydatetime(),
            "validStart": row.validStart.to_pydatetime(),
            "price_raw": float(row.price_raw),
            "price_adjusted": float(row.price_adjusted),
            "volume": int(row.volume),
            "eventTags": row.eventTags
        }
        docs.append(doc)

    # Replace existing data
    db[TS_COLL].delete_many({"meta.symbol": SYMBOL})
    if docs:
        for i in range(0, len(docs), 10_000):
            db[TS_COLL].insert_many(docs[i:i+10_000], ordered=True)

    print(f"Inserted {len(docs)} docs into time-series collection {TS_COLL}.")

if __name__ == "__main__":
    main()
