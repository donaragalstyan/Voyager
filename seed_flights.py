#!/usr/bin/env python3
"""
Seed flight_catalog and flight_prices from the Kaggle "Flight Price
Prediction" dataset (shubhambathwal/flight-price-prediction).

Download it yourself first (requires a Kaggle account + API token):
    pip install kaggle
    kaggle datasets download -d shubhambathwal/flight-price-prediction -p ./data --unzip

That produces Clean_Dataset.csv with columns:
    Unnamed: 0, airline, flight, source_city, departure_time, stops,
    arrival_time, destination_city, class, duration, days_left, price

Notes on this dataset's quirks, handled below:
  - departure_time / arrival_time are TIME-OF-DAY BUCKETS
    ("Morning", "Early_Morning", "Evening", "Night", "Late_Night",
    "Afternoon"), not clock times. We map each bucket to a representative
    TIME value since flight_catalog.departure_time/arrival_time are TIME.
  - stops is text ("zero", "one", "two_or_more") -> mapped to an integer.
  - duration is decimal hours (e.g. 2.17) -> converted to minutes.
  - source_city/destination_city are Indian metro names -> mapped to IATA
    codes for the 6 cities the dataset covers.
  - days_left is "days until departure" at scrape time, not a real date.
    We synthesize travel_date as (run date + days_left) so flight_prices
    gets a spread of future dates. Re-running the script on a different
    day will produce a different (but still valid) spread.

Usage:
    python3 seed_flights.py --csv ./data/Clean_Dataset.csv --dry-run
    python3 seed_flights.py --csv ./data/Clean_Dataset.csv --database-url postgresql://user:pass@localhost:5432/trip_planner
"""

import argparse
import datetime
import sys

import pandas as pd

CITY_TO_IATA = {
    "Delhi": "DEL",
    "Mumbai": "BOM",
    "Bangalore": "BLR",
    "Kolkata": "CCU",
    "Hyderabad": "HYD",
    "Chennai": "MAA",
}

STOPS_TO_INT = {
    "zero": 0,
    "one": 1,
    "two_or_more": 2,
}

# Representative clock time for each time-of-day bucket in the dataset.
BUCKET_TO_TIME = {
    "Early_Morning": "05:00",
    "Morning": "08:00",
    "Afternoon": "13:00",
    "Evening": "18:00",
    "Night": "21:00",
    "Late_Night": "23:30",
}


def load_and_transform(csv_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    required = {
        "airline", "flight", "source_city", "departure_time", "stops",
        "arrival_time", "destination_city", "class", "duration",
        "days_left", "price",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing expected columns: {missing}")

    df["origin_airport"] = df["source_city"].map(CITY_TO_IATA)
    df["destination_airport"] = df["destination_city"].map(CITY_TO_IATA)
    unmapped = df[df["origin_airport"].isna() | df["destination_airport"].isna()]
    if len(unmapped):
        print(f"WARNING: dropping {len(unmapped)} rows with unrecognized city names", file=sys.stderr)
        df = df.dropna(subset=["origin_airport", "destination_airport"])

    df["departure_time_clock"] = df["departure_time"].map(BUCKET_TO_TIME)
    df["arrival_time_clock"] = df["arrival_time"].map(BUCKET_TO_TIME)
    df["stops_int"] = df["stops"].map(STOPS_TO_INT).fillna(0).astype(int)
    df["duration_minutes"] = (df["duration"].astype(float) * 60).round().astype(int)
    df["cabin_class"] = df["class"].str.lower()

    catalog = df[[
        "airline", "flight", "origin_airport", "destination_airport",
        "departure_time_clock", "arrival_time_clock", "duration_minutes",
        "stops_int", "cabin_class",
    ]].rename(columns={
        "flight": "flight_number",
        "departure_time_clock": "departure_time",
        "arrival_time_clock": "arrival_time",
        "stops_int": "stops",
    }).drop_duplicates(
        subset=["airline", "flight_number", "origin_airport", "destination_airport", "cabin_class"]
    ).reset_index(drop=True)

    today = datetime.date.today()
    prices = df[["airline", "flight", "origin_airport", "destination_airport", "cabin_class", "days_left", "price"]].copy()
    prices["travel_date"] = prices["days_left"].apply(lambda d: today + datetime.timedelta(days=int(d)))
    prices = prices.rename(columns={"flight": "flight_number"}).drop(columns=["days_left"])
    # dataset has repeat (route, date) rows at different prices (different search snapshots) -
    # keep the latest one seen per (flight, date) to satisfy the UNIQUE constraint.
    prices = prices.drop_duplicates(
        subset=["airline", "flight_number", "origin_airport", "destination_airport", "cabin_class", "travel_date"],
        keep="last",
    ).reset_index(drop=True)

    return catalog, prices


def insert(database_url: str, catalog: pd.DataFrame, prices: pd.DataFrame) -> None:
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(database_url)
    try:
        with conn, conn.cursor() as cur:
            catalog_id_map = {}
            for row in catalog.itertuples(index=False):
                cur.execute(
                    """
                    INSERT INTO flight_catalog
                        (airline, flight_number, origin_airport, destination_airport,
                         departure_time, arrival_time, duration_minutes, stops, cabin_class)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (airline, flight_number, origin_airport, destination_airport, cabin_class)
                    DO UPDATE SET departure_time = EXCLUDED.departure_time
                    RETURNING flight_id, airline, flight_number, origin_airport, destination_airport, cabin_class
                    """,
                    (
                        row.airline, row.flight_number, row.origin_airport, row.destination_airport,
                        row.departure_time, row.arrival_time, row.duration_minutes, row.stops, row.cabin_class,
                    ),
                )
                fid, *key = cur.fetchone()
                catalog_id_map[tuple(key)] = fid

            price_rows = []
            for row in prices.itertuples(index=False):
                key = (row.airline, row.flight_number, row.origin_airport, row.destination_airport, row.cabin_class)
                fid = catalog_id_map.get(key)
                if fid is None:
                    continue
                price_rows.append((fid, row.travel_date, row.price))

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO flight_prices (flight_id, travel_date, price)
                VALUES %s
                ON CONFLICT (flight_id, travel_date) DO UPDATE SET
                    price = EXCLUDED.price,
                    last_refreshed_at = now()
                """,
                price_rows,
            )
        print(f"Inserted/updated {len(catalog)} flight_catalog rows and {len(price_rows)} flight_prices rows.")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", required=True, help="Path to Clean_Dataset.csv")
    parser.add_argument("--database-url", help="postgresql://user:pass@host:port/dbname")
    parser.add_argument("--dry-run", action="store_true", help="Transform only, print summary, skip DB insert")
    args = parser.parse_args()

    catalog, prices = load_and_transform(args.csv)

    if args.dry_run or not args.database_url:
        print(f"catalog rows: {len(catalog)}")
        print(catalog.head())
        print(f"\nprice rows: {len(prices)}")
        print(prices.head())
        if not args.database_url:
            print("\n--database-url not provided; skipping insert.")
        return

    insert(args.database_url, catalog, prices)


if __name__ == "__main__":
    main()
