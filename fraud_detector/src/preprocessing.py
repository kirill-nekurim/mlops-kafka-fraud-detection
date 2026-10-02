import numpy as np
import pandas as pd

# категориальные признаки, catboost сам их закодирует
CAT_FEATURES = ['merch', 'cat_id', 'gender', 'one_city', 'us_state', 'jobs']
NUM_FEATURES = ['amount_log', 'population_log', 'distance', 'hour', 'day_of_week', 'month']
FEATURES = CAT_FEATURES + NUM_FEATURES


def haversine(lat1, lon1, lat2, lon2):
    # расстояние между клиентом и мерчантом в км
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(a))


def run_preproc(df):
    df = df.copy()

    # временные признаки
    t = pd.to_datetime(df['transaction_time'])
    df['hour'] = t.dt.hour
    df['day_of_week'] = t.dt.dayofweek
    df['month'] = t.dt.month

    # гео признак
    for col in ['lat', 'lon', 'merchant_lat', 'merchant_lon']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['distance'] = haversine(df['lat'], df['lon'], df['merchant_lat'], df['merchant_lon'])

    # логарифмируем суммы и население
    df['amount_log'] = np.log1p(pd.to_numeric(df['amount'], errors='coerce'))
    df['population_log'] = np.log1p(pd.to_numeric(df['population_city'], errors='coerce'))

    # пропуски в категориях заменяем строкой
    for col in CAT_FEATURES:
        df[col] = df[col].fillna('NAN').astype(str)

    return df[FEATURES]
