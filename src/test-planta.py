from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import pytz

from dotenv import load_dotenv
load_dotenv()

pd.set_option('display.max_columns', None)

client = MongoClient(os.getenv('MONGO_URI'))
db = client['wapsi']
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)

# df_reportes = pd.read_csv('../data/reportes.csv')

def moda_data(data):
    if len(data) == 0:
        return ""
    
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

df_prog = pd.read_csv('../data/prog.csv')
df_prog['date'] = pd.to_datetime(df_prog['date'])
df_prog['year'] = df_prog['year']

tripsPlanta = db['plantas']
df_planta = pd.DataFrame(list(tripsPlanta.find()))
df_planta['_id'] = df_planta['_id'].astype(str)
df_planta.fillna('-', inplace=True)
df_planta['dominio'] = df_planta['dominio'].apply(lambda x: moda_data(x))
df_planta['tajo'] = df_planta['tajo'].apply(lambda x: moda_data(x))
df_planta['zona'] = df_planta['zona'].apply(lambda x: moda_data(x))
df_planta['veta'] = df_planta['veta'].apply(lambda x: moda_data(x))

arr = ['year', 'month']

def leyPonderada(x):
    return np.average(x, weights=df_planta.loc[x.index, 'tonh'])

df_planta.groupby(arr).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada})