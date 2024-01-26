from flask import Flask, jsonify, request
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd
import numpy as np
import json
import os
client = MongoClient(os.getenv('MONGO_URI'))
pd.set_option('display.max_columns', None)

db = client['wapsi']
trips = db['listtrips']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)
df_cancha= df_trips.query('status != "Planta"')
df_ruma = df_cancha.groupby(['ubication', 'mining', 'dominio', 'cod_tableta']).agg({'tonh': 'sum', 'tmh_ag': 'sum', 'tajo': ['unique'], '_id': ['unique']}).reset_index()
df_ruma.columns = ['ubication', 'mining', 'dominio', 'cod_tableta', 'tonh', 'tmh_ag', 'tajo', 'travels']
df_ruma['n_travels'] = df_ruma['travels'].apply(lambda x: len(x))
df_ruma['ley_ag'] = df_ruma['tmh_ag'] / df_ruma['tonh']
# df_ruma.to_json('ruma.json', orient='records')

rumas = db['rumas']
df_ruma = pd.DataFrame(list(rumas.find()))