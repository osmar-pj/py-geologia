from flask import Flask, jsonify, request, Response
# from flask_pymongo import PyMongo
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
# pd.set_option('display.max_rows', None)

def moda_data(data):
    isArray = isinstance(data, list)
    if isArray:
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

df_reportes = pd.read_csv('../data/reportes.csv')
df_prog = pd.read_csv('../data/prog.csv')
df_prog['date'] = pd.to_datetime(df_prog['date'])
df_prog['year'] = df_prog['year']

trips = db['trips']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)
df = df_trips

ts = 1708398742
category = "trips"
arr = ["rango", "type"]

fixed = ["year", "month", "mining"]
months = ["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"]
month = months[datetime.fromtimestamp(ts).month - 1]
year = datetime.fromtimestamp(ts).year
if category == "planta":
    df['dominio'] = df['dominio'].apply(lambda x: moda_data(x))
    df['tajo'] = df['tajo'].apply(lambda x: moda_data(x))
    df['zona'] = df['zona'].apply(lambda x: moda_data(x))
#     df['veta'] = df['veta'].apply(lambda x: moda_data(x))
def leyPonderada(x):
    return np.average(x, weights=df.loc[x.index, 'tonh'])

both = [
    {"mining": "YUMPAG"},
    {"mining": "UCHUCCHACUA"}
]
# grouped is concat fixed and arr
grouped = fixed + arr
df_body = df.groupby(grouped).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

data = []
for i in range(len(both)):
    df_bodyFiltered = df_body[( df_body['year'] == year) & (df_body['month'] == month) & (df_body['mining'] == both[i]['mining'])]
    df_footer = df_bodyFiltered.groupby("year").agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
    # concat body and footer
    df_footer['year'] = "TOTAL"
    _df = pd.concat([df_bodyFiltered, df_footer])
    _df.replace(np.nan, None, inplace=True)
    print(_df, i)
    body = _df.to_dict('records')
    data.append({"body": body})
