#!/usr/bin/env python3
"""
Generate synthetic hotel_catalog and hotel_prices data.

There's no single Kaggle dataset that cleanly matches hotel_catalog's
schema (name, city, star rating, amenities, lat/long) plus a per-night
price series, so this generates synthetic-but-plausible data instead,
scoped to the same 6 cities as the flight dataset (seed_flights.py) so
a demo itinerary can actually connect a flight to a hotel in the same city.

Usage:
    python3 seed_hotels.py --hotels-per-city 15 --days 30 --dry-run
    python3 seed_hotels.py --hotels-per-city 15 --days 30 --database-url postgresql://user:pass@localhost:5432/trip_planner
"""

import argparse
import datetime
import random

import pandas as pd

CITIES = {
    "Delhi": "India",
    "Mumbai": "India",
    "Bangalore": "India",
    "Kolkata": "India",
    "Hyderabad": "India",
    "Chennai": "India",
}

# Roughly the real city centers, for plausible (not precise) lat/long jitter.
CITY_COORDS = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Bangalore": (12.9716, 77.5946),
    "Kolkata": (22.5726, 88.3639),
    "Hyderabad": (17.3850, 78.4867),
    "Chennai": (13.0827, 80.2707),
}

NAME_PREFIXES = ["Grand", "Royal", "The", "Park", "Golden", "Blue Sky", "Sunrise", "Heritage", "City", "Palm"]
NAME_SUFFIXES = ["Hotel", "Palace", "Suites", "Residency", "Inn", "Towers", "Plaza", "Grand", "Retreat"]

AMENITY_POOL = [
    "wifi", "pool", "gym", "spa", "breakfast_included", "parking",
    "airport_shuttle", "restaurant", "bar", "room_service", "pet_friendly",
    "air_conditioning", "business_center",
]

BASE_PRICE_BY_STAR = {1: 25, 2: 40, 3: 65, 4: 110, 5: 200}


def generate_catalog(hotels_per_city: int, seed: int) -> pd.DataFrame:
    rng = random.Random(seed)
    rows = []
    for city, country in CITIES.items():
        lat0, lon0 = CITY_COORDS[city]
        for _ in range(hotels_per_city):
            star = rng.choice([2, 3, 3, 4, 4, 5])  # weighted toward 3-4 star
            name = f"{rng.choice(NAME_PREFIXES)} {city} {rng.choice(NAME_SUFFIXES)}"
            amenities = rng.sample(AMENITY_POOL, k=rng.randint(3, 7))
            rows.append({
                "name": name,
                "city": city,
                "country": country,
                "address": f"{rng.randint(1, 200)} {rng.choice(['MG Road', 'Main Street', 'Station Road', 'Park Avenue', 'Ring Road'])}, {city}",
                "star_rating": star,
                "latitude": round(lat0 + rng.uniform(-0.15, 0.15), 6),
                "longitude": round(lon0 + rng.uniform(-0.15, 0.15), 6),
                "amenities": amenities,
                "base_price": BASE_PRICE_BY_STAR[star] * rng.uniform(0.85, 1.25),
            })
    return pd.DataFrame(rows)


def generate_prices(catalog: pd.DataFrame, days: int, seed: int) -> pd.DataFrame:
    rng = random.Random(seed + 1)
    today = datetime.date.today()
    rows = []
    for hotel in catalog.itertuples(index=False):
        for d in range(days):
            travel_date = today + datetime.timedelta(days=d)
            # weekend bump + small daily noise, mimicking real demand-based pricing
            is_weekend = travel_date.weekday() >= 5
            noise = rng.uniform(0.9, 1.15)
            weekend_mult = 1.2 if is_weekend else 1.0
            price = round(hotel.base_price * weekend_mult * noise, 2)
            rows.append({"hotel_name": hotel.name, "city": hotel.city, "stay_date": travel_date, "price_per_night": price})
    return pd.DataFrame(rows)


def insert(database_url: str, catalog: pd.DataFrame, prices: pd.DataFrame) -> None:
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(database_url)
    try:
        with conn, conn.cursor() as cur:
            hotel_id_map = {}
            for row in catalog.itertuples(index=False):
                cur.execute(
                    """
                    INSERT INTO hotel_catalog
                        (name, city, country, address, star_rating, latitude, longitude, amenities)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING hotel_id
                    """,
                    (
                        row.name, row.city, row.country, row.address,
                        row.star_rating, row.latitude, row.longitude, row.amenities,
                    ),
                )
                hotel_id_map[(row.name, row.city)] = cur.fetchone()[0]

            price_rows = []
            for row in prices.itertuples(index=False):
                hid = hotel_id_map.get((row.hotel_name, row.city))
                if hid is None:
                    continue
                price_rows.append((hid, row.stay_date, row.price_per_night))

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO hotel_prices (hotel_id, stay_date, price_per_night)
                VALUES %s
                ON CONFLICT (hotel_id, stay_date) DO UPDATE SET
                    price_per_night = EXCLUDED.price_per_night,
                    last_refreshed_at = now()
                """,
                price_rows,
            )
        print(f"Inserted {len(catalog)} hotel_catalog rows and {len(price_rows)} hotel_prices rows.")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--hotels-per-city", type=int, default=15)
    parser.add_argument("--days", type=int, default=30, help="How many days of forward pricing to generate per hotel")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--database-url", help="postgresql://user:pass@host:port/dbname")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    catalog = generate_catalog(args.hotels_per_city, args.seed)
    prices = generate_prices(catalog, args.days, args.seed)

    if args.dry_run or not args.database_url:
        print(f"catalog rows: {len(catalog)}")
        print(catalog.head())
        print(f"\nprice rows: {len(prices)}")
        print(prices.head())
        if not args.database_url:
            print("\n--database-url not provided; skipping insert.")
        return

    insert(args.database_url, catalog.drop(columns=["base_price"]), prices)


if __name__ == "__main__":
    main()
