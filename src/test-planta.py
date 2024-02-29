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

def moda_data(data):
    isArray = isinstance(data, list)
    if not isArray:
        return data
    data_count = {}
    max_count = 0
    moda = 0
    for i in data:
        if i in data_count:
            data_count[i] += 1
        else:
            data_count[i] = 1 
    for key, value in data_count.items():
        if value > max_count:
            moda = key
            max_count = value  
    return moda

# timestamp = 1625097600
ts = '1706509758'
arr = ['turn', 'tajo']
category = 'planta'

months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
idxMonth = datetime.fromtimestamp(int(ts)).month - 1
month = months[idxMonth]
year = datetime.fromtimestamp(int(ts)).year

trips = db['plantas']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)
fixed = ['year','month']
if category == "planta":
        df_trips['dominio'] = df_trips['dominio'].apply(lambda x: moda_data(x))
        df_trips['tajo'] = df_trips['tajo'].apply(lambda x: moda_data(x))
        df_trips['zona'] = df_trips['zona'].apply(lambda x: moda_data(x))
        df_trips['veta'] = df_trips['veta'].apply(lambda x: moda_data(x))
def leyPonderada(x):
        return np.average(x, weights=df_trips.loc[x.index, 'tonh'])

# grouped is concat fixed and arr
grouped = fixed + arr
values = ['tonh', 'ley_ag', 'ley_fe', 'ley_mn', 'ley_pb', 'ley_zn']
df_body = df_trips.groupby(grouped).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

df_bodyFiltered = df_body[( df_body['year'] == year) & (df_body['month'] == month)]
df_footer = df_bodyFiltered.groupby("year").agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
# concat body and footer
df_footer['year'] = "TOTAL"
_df = pd.concat([df_bodyFiltered, df_footer])
_df.replace(np.nan, None, inplace=True)