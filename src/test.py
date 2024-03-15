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
ts = '1704085200'  # ENERO
# ts = '1706763600'  # FEBRERO
mining = 'YUMPAG'
# df_geology, df_main, df_prog = getGeology()

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

def nsr(df):
    df['ag_rec'] = [0.28877 * x if x < 2.8 else 0.0422 * np.log(x) + 0.768505 for x in df['ley_ag']]
    df['pb_rec'] = [2.2829 * x if x < 0.4 else 0.0024 * x + 0.896 for x in df['ley_pb']]
    df['zn_rec'] = [0.81564 * x if x < 0.55 else 0.14627 * np.log(x) + 0.60619 if x < 7.85 else 0.808 for x in df['ley_zn']]
    df['nsr'] = df['ag_rec'] * pointValues['vp_ag'] * df['ley_ag'] + df['pb_rec'] * pointValues['vp_pb'] * df['ley_pb'] + df['zn_rec'] * pointValues['vp_zn'] * df['ley_zn']
    df['ag_eq'] = df['nsr'] / (pointValues['vp_ag'] * df['ag_rec'])
    return df


pointValues = {
    'vp_ag': 13,
    'vp_pb': 14.69,
    'vp_zn': 13.76,
}

# timestamp = 1625097600
# ts = '1706509758'
ts = '1706763600' # FEBRERO
# ts = '1704085200' # ENERO
# ts = '1704085200' # MARZO
mining = "YUMPAG"

months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO','JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE','DICIEMBRE']
idxMonth = datetime.fromtimestamp(int(ts)).month - 1
month = months[idxMonth]

trips = db['trips']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)

prog = db['prog_canchas']
df_prog = pd.DataFrame(list(prog.find()))
df_prog['_id'] = df_prog['_id'].astype(str)
df_prog['date'] = pd.to_datetime(df_prog['date'])
df_prog.sort_values(by=['date'], inplace=True)

year = datetime.fromtimestamp(int(ts)).year
def leyPonderada(x):
        return np.average(x, weights=df_trips.loc[x.index, 'tonh'])
# get just date in df['date]
df_trips['date'] = df_trips['date'].apply(lambda x: x.strftime('%d/%m/%Y'))
df_trips_filtered = df_trips.query('month == @month and year == @year and mining == @mining').dropna(subset=['ley_ag', 'ley_fe', 'ley_mn', 'ley_pb', 'ley_zn'])
df1 = df_trips_filtered.groupby(['date']).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

df1['date'] = pd.to_datetime(df1['date'], format='%d/%m/%Y')
df2 = df_prog.query('month == @month and year == @year and mining == @mining')

if len(df2) == 0:
    df3 = df1.copy()
    df3['timestamp'] = df3['date'].apply(lambda x: x.timestamp())
    df3['ton_prog'] = 0
    df3['ley_ag_prog'] = 0
    df3['ley_fe_prog'] = 0
    df3['ley_mn_prog'] = 0
    df3['ley_pb_prog'] = 0
    df3['ley_zn_prog'] = 0
    df3.sort_values(by=['timestamp'], inplace=True)
else:
    df3 = pd.merge(df2, df1, on='date', how='left')
    df3['timestamp'] = df3['date'].apply(lambda x: datetime.strptime(x.strftime('%d/%m/%Y'), '%d/%m/%Y').timestamp())
    df3.sort_values(by=['timestamp'], inplace=True)
    df3.replace(np.nan, None, inplace=True)
now = datetime.now()
nowTimestamp = now.timestamp()
idxNonthNow = datetime.fromtimestamp(int(ts)).month - 1
monthNow = months[idxNonthNow]
yearNow = now.year

total_ton_prog = df3['ton_prog'].sum()
total_ton_ejec_cumm = df3.query('timestamp < @nowTimestamp')['tonh'].sum()
total_ton_prog_cumm = df3.query('timestamp < @nowTimestamp')['ton_prog'].sum()

df4 = df3.fillna(0)
df5 = df4.query('timestamp < @nowTimestamp').copy()

total_ley_ag_prog = np.average(df4['ley_ag_prog'], weights=df4['ton_prog'])
total_ley_ag_prog_cumm = np.average(df5['ley_ag_prog'], weights=df5['ton_prog'])
total_ley_ag_ejec_cumm = np.average(df5['ley_ag'], weights=df5['tonh'])

total_ley_fe_prog = np.average(df4['ley_fe_prog'], weights=df4['ton_prog'])
total_ley_fe_prog_cumm = np.average(df5['ley_fe_prog'], weights=df5['ton_prog'])
total_ley_fe_ejec_cumm = np.average(df5['ley_fe'], weights=df5['tonh'])

total_ley_mn_prog = np.average(df4['ley_mn_prog'], weights=df4['ton_prog'])
total_ley_mn_prog_cumm = np.average(df5['ley_mn_prog'], weights=df5['ton_prog'])
total_ley_mn_ejec_cumm = np.average(df5['ley_mn'], weights=df5['tonh'])

total_ley_pb_prog = np.average(df4['ley_pb_prog'], weights=df4['ton_prog'])
total_ley_pb_prog_cumm = np.average(df5['ley_pb_prog'], weights=df5['ton_prog'])
total_ley_pb_ejec_cumm = np.average(df5['ley_pb'], weights=df5['tonh'])

total_ley_zn_prog = np.average(df4['ley_zn_prog'], weights=df4['ton_prog'])
total_ley_zn_prog_cumm = np.average(df5['ley_zn_prog'], weights=df5['ton_prog'])
total_ley_zn_ejec_cumm = np.average(df5['ley_zn'], weights=df5['tonh'])

meta = {
    'ton': {
        'total_ton_prog': total_ton_prog,
        'total_ton_ejec_cumm': total_ton_ejec_cumm,
        'total_ton_prog_cumm': total_ton_prog_cumm,
        'percent_ejec': total_ton_ejec_cumm * 100 / total_ton_prog,
        'percent_prog': total_ton_prog_cumm * 100 / total_ton_prog
    },
    'ley_ag': {
        'total_ley_prog': total_ley_ag_prog,
        'total_ley_ejec_cumm': total_ley_ag_ejec_cumm,
        'total_ley_prog_cumm': total_ley_ag_prog_cumm,
        'percent_ejec': total_ley_ag_ejec_cumm * 100 / total_ley_ag_prog,
        'percent_prog': total_ley_ag_prog_cumm * 100 / total_ley_ag_prog
    },
    'ley_fe': {
        'total_ley_prog': total_ley_fe_prog,
        'total_ley_ejec_cumm': total_ley_fe_ejec_cumm,
        'total_ley_prog_cumm': total_ley_fe_prog_cumm,
        'percent_ejec': total_ley_fe_ejec_cumm * 100 / total_ley_fe_prog,
        'percent_prog': total_ley_fe_prog_cumm * 100 / total_ley_fe_prog
    },
    'ley_mn': {
        'total_ley_prog': total_ley_mn_prog,
        'total_ley_ejec_cumm': total_ley_mn_ejec_cumm,
        'total_ley_prog_cumm': total_ley_mn_prog_cumm,
        'percent_ejec': total_ley_mn_ejec_cumm * 100 / total_ley_mn_prog,
        'percent_prog': total_ley_mn_prog_cumm * 100 / total_ley_mn_prog
    },
    'ley_pb': {
        'total_ley_prog': total_ley_pb_prog,
        'total_ley_ejec_cumm': total_ley_pb_ejec_cumm,
        'total_ley_prog_cumm': total_ley_pb_prog_cumm,
        'percent_ejec': total_ley_pb_ejec_cumm * 100 / total_ley_pb_prog,
        'percent_prog': total_ley_pb_prog_cumm * 100 / total_ley_pb_prog
    },
    'ley_zn': {
        'total_ley_prog': total_ley_zn_prog,
        'total_ley_ejec_cumm': total_ley_zn_ejec_cumm,
        'total_ley_prog_cumm': total_ley_zn_prog_cumm,
        'percent_ejec': total_ley_zn_ejec_cumm * 100 / total_ley_zn_prog,
        'percent_prog': total_ley_zn_prog_cumm * 100 / total_ley_zn_prog
    },
}