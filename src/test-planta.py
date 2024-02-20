from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import pytz

from dotenv import load_dotenv
load_dotenv()
client = MongoClient(os.getenv('MONGO_URI'))

db = client['wapsi']
pd.set_option('display.max_columns', None)
# timestamp = 1625097600
ts = '1704234233'
mining = 'YUMPAG'
months = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
idxMonth = datetime.fromtimestamp(int(ts)).month - 1
month = months[idxMonth]
year = datetime.fromtimestamp(int(ts)).year
print(month, year)

trips = db['plantas']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)

df_prog = pd.read_csv('../data/prog_planta.csv')
df_finos = pd.read_csv('../data/finos_planta.csv')
df_lab = pd.read_csv('../data/lab_planta.csv')

df_prog.fillna(np.nan, inplace=True)
df_finos.fillna(np.nan, inplace=True)
df_lab.fillna(np.nan, inplace=True)

df_prog.replace(np.nan, None, inplace=True)
df_finos.replace(np.nan, None, inplace=True)
df_lab.replace(np.nan, None, inplace=True)

df_prog['date'] = pd.to_datetime(df_prog['date'])
df_finos['date'] = pd.to_datetime(df_finos['date'])
df_lab['date'] = pd.to_datetime(df_lab['date'])

def leyPonderada(x):
        return np.average(x, weights=df_trips.loc[x.index, 'tonh'])

df_planta = df_trips.groupby('date').agg({'ton': 'sum', 'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

# # concat all dataframes
df_planta = df_planta.merge(df_prog, on='date', how='left')
df_planta = df_planta.merge(df_finos, on='date', how='left')
df_planta = df_planta.merge(df_lab, on='date', how='left')
df_planta['month'] = df_planta['date'].dt.month
df_planta['month'] = df_planta['month'].apply(lambda x: months[x-1])
df_planta['year'] = df_planta['date'].dt.year
df_planta['timestamp'] = df_planta['date'].apply(lambda x: x.timestamp())  

_df = df_planta[(df_planta['month'] == month) & (df_planta['year'] == year)]
