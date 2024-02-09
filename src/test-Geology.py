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

df_reportes = pd.read_csv('../data/reportes.csv')
df_prog = pd.read_csv('../data/prog.csv')
df_prog['date'] = pd.to_datetime(df_prog['date'])
df_prog['year'] = df_prog['year']

trips = db['trips']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)
arr = ['year', 'month', 'rango', 'tajo']
months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO','JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE','DICIEMBRE']
actualMonth = datetime.now().month
nameMonth = 'ENERO' #months[actualMonth - 1]
wordM = 'month'
areWordsIn = wordM in arr
if len(arr) > 0:
    month = 'month'
    idx = [i for i, s in enumerate(arr) if month in s]
    nro_month = 'nro_month'
    if len(idx) > 0:
        arr.insert(idx[0], nro_month)
    df_result = df_trips.groupby(arr).agg(ton=('ton', 'sum'), tonh=('tonh', 'sum'), ley_ag=('ley_ag', 'mean'), ley_fe=('ley_fe', 'mean'), ley_mn=('ley_mn', 'mean'), ley_pb=('ley_pb', 'mean'), ley_zn=('ley_zn', 'mean'), tmh_ag=('tmh_ag', 'sum'), tmh_fe=('tmh_fe', 'sum'), tmh_mn=('tmh_mn', 'sum'), tmh_pb=('tmh_pb', 'sum'), tmh_zn=('tmh_zn', 'sum')).reset_index()
    if areWordsIn:
        df_result = df_result.query('month == @nameMonth')
    # result = df_result.to_dict('records')
