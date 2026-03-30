import requests
import pandas as pd



def extract_weather(start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": 30.2672,
        "longitude": -97.7431,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,precipitation_sum",
        "timezone": "auto"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

   
    if "daily" not in data:
        print("API ERROR:", data)
        return pd.DataFrame()

    df = pd.DataFrame({
        "date": data['daily']['time'],
        "temperature": data['daily']['temperature_2m_max'],
        "precipitation": data['daily']['precipitation_sum']
    })

    return df

def extract_weather_from_db():
    import pandas as pd
    import psycopg2
    from config.config import WEATHER_DB_CONFIG

    conn = psycopg2.connect(**WEATHER_DB_CONFIG)

    df = pd.read_sql("SELECT * FROM weather_data", conn)

    conn.close()

    return df
