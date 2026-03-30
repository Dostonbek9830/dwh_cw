import psycopg2
from config.config import DB_CONFIG

def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # DROP
    cur.execute("DROP TABLE IF EXISTS fact_parking_transaction CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_date CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_time CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_location CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_source CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_payment CASCADE")
    cur.execute("DROP TABLE IF EXISTS dim_weather CASCADE")

    # DIM DATE
    cur.execute("""
    CREATE TABLE dim_date (
        date_id INT PRIMARY KEY,
        full_date DATE,
        day INT,
        month INT,
        year INT,
        weekday INT
    )
    """)

    # DIM TIME
    cur.execute("""
    CREATE TABLE dim_time (
        time_id INT PRIMARY KEY,
        hour INT,
        minute INT
    )
    """)

  
    cur.execute("""
    CREATE TABLE dim_location (
        location_id SERIAL PRIMARY KEY,
        kiosk_id TEXT,
        app_zone_id TEXT,
        location_group TEXT,
        UNIQUE(kiosk_id, app_zone_id, location_group)
    )
    """)

    cur.execute("""
    CREATE TABLE dim_source (
        source_id SERIAL PRIMARY KEY,
        source TEXT UNIQUE
    )
    """)

    cur.execute("""
    CREATE TABLE dim_payment (
        payment_id SERIAL PRIMARY KEY,
        payment_method TEXT UNIQUE
    )
    """)

    cur.execute("""
        CREATE TABLE dim_weather (
            weather_id SERIAL PRIMARY KEY,
            date_id INT UNIQUE,
            temperature FLOAT,
            precipitation FLOAT,
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        );
    """)

    # FACT
    cur.execute("""
    CREATE TABLE fact_parking_transaction (
        transaction_id BIGINT PRIMARY KEY,
        date_id INT,
        time_id INT,
        location_id INT,
        source_id INT,
        payment_id INT,
        duration INT,
        amount NUMERIC,

        FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (source_id) REFERENCES dim_source(source_id),
        FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id)
    )
    """)

    cur.execute("CREATE INDEX idx_fact_date_id ON fact_parking_transaction(date_id)")
    cur.execute("CREATE INDEX idx_fact_time_id ON fact_parking_transaction(time_id)")
    cur.execute("CREATE INDEX idx_fact_location_id ON fact_parking_transaction(location_id)")
    cur.execute("CREATE INDEX idx_fact_payment_id ON fact_parking_transaction(payment_id)")
    cur.execute("CREATE INDEX idx_dim_date_full_date ON dim_date(full_date)")
    cur.execute("CREATE INDEX idx_dim_location_group ON dim_location(location_group)")

    conn.commit()
    cur.close()
    conn.close()

    print("TABLES CREATED")
