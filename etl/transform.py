import pandas as pd

DATETIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"


def transform_data(input_path):
    df = pd.read_csv(input_path, low_memory=False, skipinitialspace=True)
    df = df.copy()
    df.columns = df.columns.str.strip()

    df['start_time'] = pd.to_datetime(df['start_time'], format=DATETIME_FORMAT, errors='coerce')
    df['end_time'] = pd.to_datetime(df['end_time'], format=DATETIME_FORMAT, errors='coerce')

    df = df.dropna(subset=['start_time']).copy()

    df['source'] = df['source'].astype(str).str.strip().str.lower()
    df['payment_method'] = df['payment_method'].astype(str).str.strip().str.lower()
    df['kiosk_id'] = df['kiosk_id'].fillna('unknown')
    df['app_zone_id'] = df['app_zone_id'].fillna('unknown')
    df['location_group'] = df['location_group'].fillna('unknown').astype(str).str.strip()
    df['date_id'] = df['start_time'].dt.strftime('%Y%m%d').astype(int)
    df['time_id'] = df['start_time'].dt.strftime('%H%M').astype(int)
    return df


def create_dim_date(df):
    dim_date = df[['date_id', 'start_time']].drop_duplicates().copy()

    dim_date['full_date'] = dim_date['start_time'].dt.date
    dim_date['day'] = dim_date['start_time'].dt.day
    dim_date['month'] = dim_date['start_time'].dt.month
    dim_date['year'] = dim_date['start_time'].dt.year
    dim_date['weekday'] = dim_date['start_time'].dt.weekday

    dim_date = dim_date.drop(columns=['start_time'])

    return dim_date

def create_dim_payment(df):
    return df[['payment_method']].drop_duplicates()


def create_dim_time(df):
    dim_time = df[['time_id', 'start_time']].drop_duplicates().copy()

    dim_time['hour'] = dim_time['start_time'].dt.hour
    dim_time['minute'] = dim_time['start_time'].dt.minute

    dim_time = dim_time.drop(columns=['start_time'])

    return dim_time



def create_dim_location(df):
    return df[['kiosk_id', 'app_zone_id', 'location_group']].drop_duplicates()



def create_dim_source(df):
    return df[['source']].drop_duplicates()



def group_payment_method(df):
    def map_payment(p):
        p = str(p).lower()

        if "apple" in p or "google" in p or "app" in p:
            return "Mobile"
        elif "coin" in p or "cash" in p:
            return "Cash"
        elif "card" in p:
            return "Card"
        else:
            return "Other"

    df["payment_method"] = df["payment_method"].apply(map_payment)

    return df



def transform_weather_to_dim(df, dim_date):
    df['date'] = pd.to_datetime(df['date'])
    df['date_id'] = df['date'].dt.strftime('%Y%m%d').astype(int)

    df = df[df['date_id'].isin(dim_date['date_id'])]

    return df[['date_id', 'temperature', 'precipitation']]


def transform_location_groups_to_dim(df):
    location_group_df = df.copy()
    location_group_df["location_group"] = (
        location_group_df["location_group"]
        .fillna("unknown")
        .astype(str)
        .str.strip()
        .replace("", "unknown")
    )
    return location_group_df[["location_group"]].drop_duplicates().reset_index(drop=True)


def create_fact_table(df):
    fact = df.copy()

    fact = fact[[
        'id',
        'date_id',
        'time_id',
        'kiosk_id',
        'app_zone_id',
        'location_group',
        'source',
        'payment_method',
        'duration_in_min',
        'amount'
    ]]
    

    return fact
   
