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
# ts = request.args.get('ts')
ts = '1706178035'
# df_geology, df_main, df_prog = getGeology()
trips = db['trips']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)
df_main= df_trips.query('status != "Planta"')
df_prog = pd.read_csv('../data/prog.csv')
df_prog['date'] = pd.to_datetime(df_prog['date'])
months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO','JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE','DICIEMBRE']
idxMonth = datetime.fromtimestamp(int(ts)).month - 1
month = months[idxMonth]
year = datetime.fromtimestamp(int(ts)).year
df_main['tonh_ley_ag'] = df_main['tonh'] * df_main['ley_ag']
df_main['tonh_ley_fe'] = df_main['tonh'] * df_main['ley_fe']
df_main['tonh_ley_mn'] = df_main['tonh'] * df_main['ley_mn']
df_main['tonh_ley_pb'] = df_main['tonh'] * df_main['ley_pb']
df_main['tonh_ley_zn'] = df_main['tonh'] * df_main['ley_zn']
df1 = df_main.query('month == @month and year == @year').groupby(['date']).agg({'tonh': 'sum', 'tonh_ley_ag': 'sum', 'tonh_ley_fe': 'sum', 'tonh_ley_mn': 'sum', 'tonh_ley_pb': 'sum', 'tonh_ley_zn': 'sum' }).reset_index()
df1['Ag'] = df1['tonh_ley_ag'] / df1['tonh']
df1['Fe'] = df1['tonh_ley_fe'] / df1['tonh']
df1['Mn'] = df1['tonh_ley_mn'] / df1['tonh']
df1['Pb'] = df1['tonh_ley_pb'] / df1['tonh']
df1['Zn'] = df1['tonh_ley_zn'] / df1['tonh']
df1['date'] = pd.to_datetime(df1['date'], format='%d/%m/%Y')
df2 = df_prog.query('month == @month and year == @year')
if len(df2) == 0:
    df3 = df1.copy()
    df3['timestamp'] = df3['date'].apply(lambda x: x.timestamp())  
    df3['ton_prog'] = 0
    df3['ley_prog'] = 0
    df3.sort_values(by=['timestamp'], inplace=True)
else:
    df3 = pd.merge(df2, df1, on='date', how='left')
    df3['timestamp'] = df3['date'].apply(lambda x: datetime.strptime(x.strftime('%d/%m/%Y'), '%d/%m/%Y').timestamp())
    df3.sort_values(by=['timestamp'], inplace=True)
    df3.replace(np.nan, None, inplace=True)
total_ton_prog = df3['ton_prog'].sum()
total_ton = df3['tonh'].sum()
aver_ley_prog = (df3['ton_prog'] * df3['ley_prog']).sum() / df3['ton_prog'].sum()
aver_ley = (df3['tonh'] * df3['Ag']).sum() / df3['tonh'].sum()

meta = {
    'total_ton_prog': total_ton_prog,
    'total_ton': total_ton,
    'aver_ley_prog': aver_ley_prog,
    'aver_ley': aver_ley
}