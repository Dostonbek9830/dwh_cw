import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from config.config import DB_CONFIG, WEATHER_DB_CONFIG


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def _make_engine(config):
    url = URL.create(
        "postgresql+psycopg2",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=int(config["port"]),
        database=config["database"],
    )
    return create_engine(url)


def _bulk_insert(conn, query, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, query, rows)
    conn.commit()


def load_dim_date(df):
    rows = [
        (
            int(row.date_id),
            row.full_date,
            int(row.day),
            int(row.month),
            int(row.year),
            int(row.weekday),
        )
        for row in df.itertuples(index=False)
    ]
    conn = get_connection()
    _bulk_insert(
        conn,
        """
        INSERT INTO dim_date (date_id, full_date, day, month, year, weekday)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    conn.close()


def load_dim_time(df):
    rows = [
        (int(row.time_id), int(row.hour), int(row.minute))
        for row in df.itertuples(index=False)
    ]
    conn = get_connection()
    _bulk_insert(
        conn,
        """
        INSERT INTO dim_time (time_id, hour, minute)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    conn.close()


def load_dim_location(df):
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    conn = get_connection()
    _bulk_insert(
        conn,
        """
        INSERT INTO dim_location (kiosk_id, app_zone_id, location_group)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    conn.close()


def load_dim_source(df):
    rows = [(row.source,) for row in df.itertuples(index=False)]
    conn = get_connection()
    _bulk_insert(
        conn,
        """
        INSERT INTO dim_source (source)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    conn.close()


def load_dim_payment(df):
    rows = [(row.payment_method,) for row in df.itertuples(index=False)]
    conn = get_connection()
    _bulk_insert(
        conn,
        """
        INSERT INTO dim_payment (payment_method)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    conn.close()

def normalize_location(df):
    normalized = df.copy()
    normalized["kiosk_id"] = pd.to_numeric(normalized["kiosk_id"], errors="coerce").fillna(-1).astype(int).astype(str)
    normalized["app_zone_id"] = pd.to_numeric(normalized["app_zone_id"], errors="coerce").fillna(-1).astype(int).astype(str)
    normalized["location_group"] = (
        normalized["location_group"]
        .fillna("unknown")
        .astype(str)
        .str.lower()
        .str.strip()
    )
    return normalized


def load_weather(df):
    weather_df = (
        df.dropna(subset=["date_id"])
        .copy()
        .sort_values("date_id")
        .drop_duplicates(subset=["date_id"], keep="last")
    )

    rows = [
        (int(row.date_id), float(row.temperature), float(row.precipitation))
        for row in weather_df.itertuples(index=False)
    ]

    conn = get_connection()
    _bulk_insert(
        conn,
        """
        INSERT INTO dim_weather (date_id, temperature, precipitation)
        VALUES %s
        ON CONFLICT (date_id) DO UPDATE
        SET temperature = EXCLUDED.temperature,
            precipitation = EXCLUDED.precipitation
        """,
        rows,
    )
    conn.close()
    print("Weather loaded into DWH")


def load_weather_to_source(df):
    rows = [
        (row.date, row.temperature, row.precipitation)
        for row in df.itertuples(index=False)
    ]
    conn = psycopg2.connect(**WEATHER_DB_CONFIG)
    _bulk_insert(
        conn,
        """
        INSERT INTO weather_data (date, temperature, precipitation)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    conn.close()
    print("Weather loaded into weather_db")

def extract_weather_from_db():
    engine = _make_engine(WEATHER_DB_CONFIG)
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM weather_data", conn)
    engine.dispose()
    return df


def get_dim_tables():
    engine = _make_engine(DB_CONFIG)
    with engine.connect() as conn:
        dim_source = pd.read_sql("SELECT * FROM dim_source", conn)
        dim_payment = pd.read_sql("SELECT * FROM dim_payment", conn)
        dim_location = pd.read_sql("SELECT * FROM dim_location", conn)
    engine.dispose()
    return dim_source, dim_payment, dim_location


def load_fact_table(fact_df, dim_location, dim_source, dim_payment):
    fact_df = normalize_location(fact_df)
    dim_location = normalize_location(dim_location)

    dim_source = dim_source.copy()
    dim_payment = dim_payment.copy()
    fact_df = fact_df.copy()

    dim_source["source"] = dim_source["source"].astype(str).str.strip().str.lower()
    dim_payment["payment_method"] = dim_payment["payment_method"].astype(str).str.strip().str.lower()
    fact_df["source"] = fact_df["source"].astype(str).str.strip().str.lower()
    fact_df["payment_method"] = fact_df["payment_method"].astype(str).str.strip().str.lower()

    fact_df = fact_df.merge(dim_source, on="source", how="left")
    fact_df = fact_df.merge(dim_payment, on="payment_method", how="left")
    fact_df = fact_df.merge(
        dim_location,
        on=["kiosk_id", "app_zone_id", "location_group"],
        how="left",
    )

    fact_df = fact_df.dropna(subset=["source_id", "payment_id", "location_id"])

    if fact_df.empty:
        raise Exception("FACT TABLE EMPTY - MERGE FAILED")

    rows = [
        (
            int(row.id),
            int(row.date_id),
            int(row.time_id),
            int(row.location_id),
            int(row.source_id),
            int(row.payment_id),
            float(row.duration_in_min),
            row.amount,
        )
        for row in fact_df.itertuples(index=False)
    ]

    conn = get_connection()
    _bulk_insert(
        conn,
        """
        INSERT INTO fact_parking_transaction
        (transaction_id, date_id, time_id, location_id, source_id, payment_id, duration, amount)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
    )
    conn.close()
    print("FACT TABLE LOADED")
