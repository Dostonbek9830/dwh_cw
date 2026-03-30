from etl.extract import extract_weather
from db.create_tables import create_tables
from etl.transform import *
from etl.load import (
    load_dim_date,
    load_dim_time,
    load_dim_location,
    load_dim_source,
    load_dim_payment,
    load_fact_table,
    get_dim_tables,
    load_weather_to_source,
    load_weather,
    extract_weather_from_db
)
from etl.transform import transform_weather_to_dim

def main():
    print("Step 1: Loading prepared dataset...")

    print("Step 2: Creating tables...")
    create_tables()
    print("Step 3: Transforming...")
    df = transform_data("data/sample/sample_parking.csv")

    df = group_payment_method(df)

    dim_date = create_dim_date(df)
    dim_time = create_dim_time(df)
    dim_location = create_dim_location(df)
    dim_source = create_dim_source(df)
    dim_payment = create_dim_payment(df)
    fact = create_fact_table(df)
    weather_start_date = str(dim_date["full_date"].min())
    weather_end_date = str(dim_date["full_date"].max())

    print("Step 4: Loading dimensions...")
    load_dim_date(dim_date)
    load_dim_time(dim_time)
    load_dim_location(dim_location)
    load_dim_source(dim_source)
    load_dim_payment(dim_payment)

    print("Step 5: Weather API → weather_db")
    weather_raw = extract_weather(weather_start_date, weather_end_date)
    load_weather_to_source(weather_raw)

    print("Step 6: Weather DB → DWH")
    weather_db_data = extract_weather_from_db()
    weather_dim = transform_weather_to_dim(weather_db_data, dim_date)
    load_weather(weather_dim)

    print("Step 7: Loading fact table...")
    db_dim_source, db_dim_payment, db_dim_location = get_dim_tables()
    load_fact_table(fact, db_dim_location, db_dim_source, db_dim_payment)

    print("ETL FINISHED")


if __name__ == "__main__":
    main()
