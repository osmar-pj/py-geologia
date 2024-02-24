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
pd.set_option('display.max_columns', None)

client = MongoClient(os.getenv('MONGO_URI'))
db = client['wapsi']

trips = db['trips']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)
df = df_trips

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

def leyPonderada(x):
        return np.average(x, weights=df.loc[x.index, 'tonh'])

df_main = df.query('status != "Planta"').reset_index(drop=True)
df_planta = df.query('status == "Planta"').reset_index(drop=True)
df_pila = df_main.groupby('cod_tableta').agg({'tajo': 'unique', 'type': 'unique', 'ubication': 'unique', 'dominio': 'unique', '_id': 'unique', 'status': 'unique', 'mining': 'unique',
        'ton': 'sum', 'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada,'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
df_planta_pila = df_planta.groupby('cod_tableta').agg({'tajo': 'unique', 'type': 'unique', 'ubication': 'unique', 'dominio': 'unique', '_id': 'unique', 'status': 'unique', 'mining': 'unique',
        'ton': 'sum', 'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada,'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
df_pila.columns = ['cod_tableta', 'tajo', 'type', 'ubication', 'dominio', 'travels', 'status', 'mining', 'ton', 'tonh', 'ley_ag', 'ley_fe', 'ley_mn', 'ley_pb', 'ley_zn']
df_planta_pila.columns = ['cod_tableta', 'tajo', 'type', 'ubication', 'dominio', 'travels', 'status', 'mining', 'ton', 'tonh', 'ley_ag', 'ley_fe', 'ley_mn', 'ley_pb', 'ley_zn']
df_pila['ubication'] = df_pila['ubication'].apply(lambda x: moda_data(x))
df_planta_pila['ubication'] = df_planta_pila['ubication'].apply(lambda x: moda_data(x))
df_pila['status'] = df_pila['status'].apply(lambda x: moda_data(x))
df_planta_pila['status'] = df_planta_pila['status'].apply(lambda x: moda_data(x))
df_pila['mining'] = df_pila['mining'].apply(lambda x: moda_data(x))
df_planta_pila['mining'] = df_planta_pila['mining'].apply(lambda x: moda_data(x))

df_pilas2 = pd.concat([df_planta_pila, df_pila], ignore_index=True)

df_pilas2.to_json('pilas2.json', orient='records')

# 61 pilas