from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta

import pandas as pd
import os
import pytz

load_dotenv()

client = MongoClient(os.getenv('MONGO_URI'))

db = client['wapsi']
pd.set_option('display.max_columns', None)

def getTripsTruck():
    df_ubications = pd.read_csv('./data/ubications.csv')
    df_trucks = pd.read_csv('./data/trucks.csv')
    
    tracking = db['trackings']
    df_data = pd.DataFrame(list(tracking.find()))

    if len(df_data) == 0:
        df_data = pd.DataFrame()

    df_data = df_data.drop(['_id'], axis=1)
    df_data['mac_wap'] = df_data['devices'].map(lambda x: x[0]['name'])
    df_data['timestamp'] = df_data['datetimeServer'].map(lambda x: x.timestamp())

    filter_points = dict(zip(df_ubications['mac'], df_ubications['name']))
    df_data['ubication'] = df_data['mac_wap'].map(filter_points)
    filter_areas = dict(zip(df_ubications['name'], df_ubications['area']))
    df_data['area'] = df_data['ubication'].map(filter_areas)
    filter_places = dict(zip(df_ubications['name'], df_ubications['place']))
    df_data['place'] = df_data['ubication'].map(filter_places)
    filter_trucks = dict(zip(df_trucks['serie'], df_trucks['name']))
    df_data['tag'] = df_data['serie'].map(filter_trucks)

    # df_data['datetime'] = pd.to_datetime(df_data['timestamp'] * 1000 , unit='ms')
    df_data['datetimeTZ'] = df_data['datetimeServer'].dt.tz_localize('UTC').dt.tz_convert('America/Lima')
    df_data['date'] = df_data['datetimeTZ'].dt.strftime('%d/%m/%Y')
    df_data['hour'] = df_data['datetimeTZ'].dt.strftime('%H:%M:%S')
    df_data['turno'] = df_data['hour'].apply(lambda x: 'DIA' if x >= '07:00:00' and x <= '19:00:00' else 'NOCHE')

    df_trucks.dropna(subset=['name'], inplace=True)
    df = pd.DataFrame()
    validateData = 120
    for i, r in df_trucks.iterrows():
        tag = r['name']
        _df = df_data.query('tag == @tag')
        _df = _df.reset_index(drop=True)
        _df['diff'] = _df['timestamp'].diff()
        _df['trip'] = _df['diff'].gt(validateData).cumsum()
        _df['trip'] = _df['trip'].apply(lambda x: x + 1)
        df = pd.concat([df, _df])

    df['datetimeMina'] = df['datetimeTZ'] + pd.Timedelta(hours=7)
    df['dateMina'] = df['datetimeMina'].dt.strftime('%d/%m/%Y')

    df_tripComplete = df.groupby(['tag', 'dateMina', 'trip', 'ubication', 'area', 'place', 'turno']).agg({'timestamp': ['min', 'max', 'count']})
    df_tripComplete.reset_index(inplace=True)
    df_tripComplete.columns = ['tag', 'date', 'trip', 'ubication', 'area', 'place', 'turno', 'min', 'max', 'count']
    df_tripComplete['datetimeMinTZ'] = pd.to_datetime(df_tripComplete['min'] * 1000 , unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
    df_tripComplete['datetimeMaxTZ'] = pd.to_datetime(df_tripComplete['max'] * 1000 , unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
    df_tripComplete['time'] = df_tripComplete['max'] - df_tripComplete['min']
    validateTrip = 30
    df_tripComplete = df_tripComplete.query('time > @validateTrip')
    df1 = pd.DataFrame()
    for i, r in df_trucks.iterrows():
        tag = r['name']
        _df = df_tripComplete.query('tag == @tag')
        _df = _df.reset_index(drop=True)
        _df.sort_values(by=['min'], inplace=True)
        _df['shift'] = _df['min'].shift(-1)
        __df = _df.copy()
        __df.dropna(subset=['shift'], inplace=True)
        __df['ubication'] = "OFFLINE"
        __df['area'] = "produccion"
        __df['place'] = "offline"
        __df['min'] = _df['max'] + 1
        __df['max'] = _df['shift'] - 1
        __df['time'] = __df['max'] - __df['min']
        __df['datetimeMinTZ'] = pd.to_datetime(__df['min'] * 1000 , unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
        __df['datetimeMaxTZ'] = pd.to_datetime(__df['max'] * 1000 , unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
        df1 = pd.concat([df1, _df, __df])

    df1.sort_values(by=['min'], inplace=True)
    validateTime = 1 # filtro por viajes muy cortos (ilogicos)
    df1['time'] = df1['time'] / 60
    df_trips = df1.query('time > @validateTime')
    df_trips.reset_index(inplace=True, drop=True)
    df_trips.drop(['datetimeMinTZ', 'datetimeMaxTZ', 'shift'], axis=1, inplace=True)
    df_trips['datetimeMina'] = pd.to_datetime(df_trips['date'], format='%d/%m/%Y')
    df_trips['datetime'] = pd.to_datetime(df_trips['min'] * 1000 , unit='ms')
    df_trips['datetimeTZ'] = df_trips['datetime'].dt.tz_localize('UTC').dt.tz_convert('America/Lima')
    df_trips['weight'] = df_trips['place'].apply(lambda x: 35 if x == 'echadero' else 0)
    df_trips['week'] = (df_trips['datetime'] - pd.Timedelta(hours=5)).apply(lambda x: get_week_number(x))
    df_trips['nro_month'] = df_trips['datetimeMina'].dt.month
    df_trips['month'] = df_trips['nro_month'].apply(lambda x: datetime(2023, x, 1).strftime('%B'))
    df_trips['month'] = df_trips['month'].replace({'January': 'Ene', 'February': 'Feb', 'March': 'Mar', 'April': 'Abr', 'May': 'May', 'June': 'Jun', 'July': 'Jul', 'August': 'Ago', 'September': 'Sep', 'October': 'Oct', 'November': 'Nov', 'December': 'Dic'})
    df_trips['hhmm'] = df_trips['datetimeTZ'].dt.strftime('%H:%M')
    df_trips['hour'] = df_trips['datetimeTZ'].dt.hour
    df_trips.to_csv('./data/tripTruck.csv', index=False)

    return df_trips

def get_week_number(date=None):
    if date is None:
        date = datetime.now()
    year = date.year
    year_start = datetime(year, 1, 1, 7, 0, 0)  # 1 de enero a las 7:00 AM

    # Establecer el inicio del año al primer jueves a las 7:00 AM
    year_start += timedelta(days=(3 - year_start.weekday() + 7) % 7)

    if year_start > date:
        year_start = datetime(year - 1, 1, 1, 7, 0, 0)
        year_start += timedelta(days=(3 - year_start.weekday() + 7) % 7)

    diff_days = (date - year_start).days
    week_number = diff_days // 7 + 1
    return week_number