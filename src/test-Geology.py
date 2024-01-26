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
df_prog['year'] = df_prog['year'].astype(str)

df_reportes.rename(columns={'MES': 'month', 'FECHA EXTRACCION': 'date', 'Año': 'year', 'ESTADO': 'status', 'UBICACIÓN': 'ubication', 'TURNO': 'turn', 'ZONA': 'mining', 'EMPRESA': 'company', 'NIVEL': 'level', 'TIPO': 'type', 'Veta': 'veta', 'Tajo': 'tajo', 'Dominio': 'dominio', 't': 'ton', 'TMH': 'tonh', 'N° carros': 'vagones', 'Ley Stope': 'ley_stope', 'CODIGO MUESTRA': 'cod_muestra', 'LEY SO': 'ley_so', 'LEY CANCHA Ag': 'ley_ag', 'LEY CANCHA Fe': 'ley_fe', 'LEY CANCHA Mn': 'ley_mn', 'LEY CANCHA Pb': 'ley_pb', 'LEY CANCHA Zn': 'ley_zn', 'Ley Original': 'ley_original', 'Ley cancha 2': 'ley_cancha2', 'Codigo Muestra 3': 'cod_muestra3', 'Ley cancha 3': 'ley_cancha3', 'LEY CANCHA castigada': 'ley_cancha_castigada', 'LEY inicial': 'ley_inicial', 'Rango': 'rango', 'Ag*TMH': 'tmh_ag', 'Fe*TMH': 'tmh_fe', 'Mn*TMH': 'tmh_mn', 'Pb*TMH': 'tmh_pb', 'Zn*TMH': 'tmh_zn', 'Codigo tableta': 'cod_tableta', 'Labor Rep.': 'labor', 'FECHA ABASTECIMIENTO': 'date_abas', 'Columna1': 'columna'}, inplace=True)

df_reportes['mining'] = df_reportes['mining'].replace('Yumpag', 'YUMPAG')

df_reportes['turn'].fillna('SIN TURNO', inplace=True)
df_reportes['level'].fillna(0, inplace=True)

df_reportes['year'] = df_reportes['year'].astype(str)

# convertir a int el level
df_reportes['level'] = df_reportes['level'].astype(int)


df_reportes['ton'].fillna(0, inplace=True)
df_reportes['tonh'].fillna(0, inplace=True)

df_reportes['vagones'] = df_reportes['vagones'].astype('Int64')
df_reportes['vagones'].fillna(0, inplace=True)
df_reportes['ley_stope'].fillna('--', inplace=True)
df_reportes['cod_muestra'].fillna('--', inplace=True)
df_reportes.drop('ley_so', axis=1, inplace=True)
df_reportes['ley_ag'].fillna(0, inplace=True)
df_reportes['ley_fe'].fillna(0, inplace=True)
df_reportes['ley_mn'].fillna(0, inplace=True)
df_reportes['ley_pb'].fillna(0, inplace=True)
df_reportes['ley_zn'].fillna(0, inplace=True)
df_reportes['ley_original'].fillna(0, inplace=True)
df_reportes['ley_cancha2'].fillna(0, inplace=True)
df_reportes['cod_muestra3'].fillna('--', inplace=True)
df_reportes['ley_cancha3'].fillna(0, inplace=True)
df_reportes['ley_cancha_castigada'].fillna(0, inplace=True)
df_reportes['ley_inicial'].fillna(0, inplace=True)
df_reportes['rango'].fillna('--', inplace=True)
df_reportes['tmh_ag'].fillna(0, inplace=True)
df_reportes['tmh_fe'].fillna(0, inplace=True)
df_reportes['tmh_mn'].fillna(0, inplace=True)
df_reportes['tmh_pb'].fillna(0, inplace=True)
df_reportes['tmh_zn'].fillna(0, inplace=True)
df_reportes['labor'].fillna('--', inplace=True)
df_reportes['cod_tableta'].fillna('--', inplace=True)
df_reportes['date_abas'].fillna('--', inplace=True)
df_reportes['columna'].fillna('--', inplace=True)

# df_reportes.groupby(['date_extraction','mining', 'month', 'year']).agg({'ton': 'sum', 'tonh': 'sum','ley_ag': 'mean', 'ley_fe': 'mean', 'ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'})

df_reportes['date'] = pd.to_datetime(df_reportes['date']).dt.strftime('%d/%m/%Y')
df_reportes['timestamp'] = df_reportes['date'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').timestamp())

df_reportes['month'] = df_reportes['month'].replace('0CTUBRE', 'OCTUBRE')

df_reportes['veta'] = df_reportes['veta'].replace('VANESA', 'Vanessa')
df_reportes['veta'] = df_reportes['veta'].replace('VANESSA', 'Vanessa')

df_reportes['veta'] = df_reportes['veta'].replace('CAMILA', 'Camila')
df_reportes['veta'] = df_reportes['veta'].replace('CACHIPAMPA', 'Cachipampa')

df_reportes['veta'] = df_reportes['veta'].replace('GINA', 'Gina')
df_reportes['veta'] = df_reportes['veta'].replace('Gina_', 'Gina')

df_reportes['veta'] = df_reportes['veta'].replace('SONIA NORTE', 'Sonia Norte')

df_reportes['status'] = df_reportes['status'].replace(['PLANTA', 'PLANTA ', 'pLANTA'], 'Planta')
df_reportes['status'] = df_reportes['status'].replace('CANCHA', 'Cancha')

df_reportes['ubication'] = df_reportes['ubication'].replace('cancha 1', 'Cancha 1')

df_reportes['tajo'] = df_reportes['tajo'].replace('AVANCE ', 'AVANCE')

df_reportes['tajo'] = df_reportes['tajo'].replace(['Sn:6431-2', 'Sn6431-2 ', 'Sn6431-2'], 'SN 6431-2')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj 400-1p', 'Tj 400-1P  ', 'Tj 400-1P'], 'TJ 400-1P')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj 400-2p', 'Tj 400-2P  '], 'TJ 400-2P')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj 500-1P', 'Tj 500-1p'], 'TJ 500-1P')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj 500-2P', 'Tj 500-2p'], 'TJ 500-2P')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj 500-3p', 'Tj 500-3P', 'Tj 500-3p', 'Tj 500-3P  '], 'TJ 500-3P')
df_reportes['tajo'] = df_reportes['tajo'].replace('Tj 500-4P', 'TJ 500-4P')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj 500-5P', 'Tj 500-5p'], 'TJ 500-5P')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj 500-6P', 'Tj 500-6p'], 'TJ 500-6P')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj 500-7p', 'Tj 500-7P', 'Tj 500-7P  '], 'TJ 500-7P')
df_reportes['tajo'] = df_reportes['tajo'].replace('Tj 500-8P', 'TJ 500-8P')

df_reportes['tajo'] = df_reportes['tajo'].replace('TJ6431', 'TJ 6431')
df_reportes['tajo'] = df_reportes['tajo'].replace(['Tj6431-1', 'TJ6431-1'], 'TJ 6431-1')
df_reportes['tajo'] = df_reportes['tajo'].replace('TJ6431-1-N', 'TJ 6431-1-N')
df_reportes['tajo'] = df_reportes['tajo'].replace('TJ6431-1-S', 'TJ 6431-1-S')

df_reportes['tajo'] = df_reportes['tajo'].replace('Tj6432-1', 'TJ 6432-1')
df_reportes['tajo'] = df_reportes['tajo'].replace('Tj6432-2', 'TJ 6432-2')


df_reportes['tajo'] = df_reportes['tajo'].replace('Tj 400-1P Baja ley ', 'TJ 400-1P Baja ley')

df_reportes['tajo'] = df_reportes['tajo'].replace('TJ6790', 'TJ 6790')
df_reportes['tajo'] = df_reportes['tajo'].replace('Tj6488', 'TJ 6488')
df_reportes['tajo'] = df_reportes['tajo'].replace('Tj 6618', 'TJ 6618')

df_reportes['week'] = pd.to_datetime(df_reportes['date'], format='%d/%m/%Y').dt.isocalendar().week
df_reportes['nro_month'] = df_reportes['month'].replace({'ENERO': 1, 'FEBRERO': 2, 'MARZO': 3, 'ABRIL': 4, 'MAYO': 5, 'JUNIO': 6, 'JULIO': 7, 'AGOSTO': 8, 'SEPTIEMBRE': 9, 'OCTUBRE': 10, 'NOVIEMBRE': 11, 'DICIEMBRE': 12})
#concatenar el nro de mes con el año
df_reportes['nro_month'] = df_reportes['nro_month'].astype(str)

df_main = df_reportes.copy()
df_main.drop(['columna', 'company', 'cod_muestra', 'vagones', 'cod_muestra3', 'ley_cancha_castigada', 'ley_stope', 'ley_inicial', 'labor', 'ley_original', 'ley_cancha2', 'ley_cancha3'], axis=1, inplace=True)

# BY MINA

# mina = 'YUMPAG'
# mina = 'UCHUCCHACUA'

# month = '1-2024'
# month = '2-2023'
# month = '3-2023'
# month = '4-2023'
# month = '5-2023'
# month = '6-2023'
# month = '7-2023'
# month = '8-2023'
# month = '9-2023'
# month = '10-2023'
# month = '11-2023'
# month = '12-2023'

# week = '1-2024'
# week = '2-2024'

# df_reportes = df_reportes.query('mining == @mina')
# df_reportes = df_reportes.query('nro_month == @month')
# df_reportes = df_reportes.query('week == @week')

# BY MONTH



# df_reportes.groupby(['week']).count()
# df_dynamic = df_reportes.groupby([df_reportes['mining'], df_reportes['year'],df_reportes['month'], df_reportes['date_extraction']]).agg({'ton': 'sum', 'tonh': 'sum','ley_ag': 'mean', 'ley_fe': 'mean', 'ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'}).reset_index()
# df_dynamic = df_reportes.groupby([df_reportes['mining'], df_reportes['year'],df_reportes['month'], df_reportes['date_extraction']]).agg({'ton': 'sum', 'tonh': 'sum','ley_ag': 'mean', 'ley_fe': 'mean', 'ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'}).reset_index()
# df_reportes.groupby([df_reportes['mining'], df_reportes['month'], df_reportes['rango'], df_reportes['veta'], df_reportes['labor']]).agg({'ton': 'sum', 'tonh': 'sum','ley_ag': 'mean', 'ley_fe': 'mean', 'ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'})
# df_reportes.pivot_table(index=['mining', 'year', 'month', 'date_extraction'],values=['ton', 'tonh', 'ley_ag', 'ley_fe', 'ley_mn', 'ley_pb', 'ley_zn'],aggfunc={'ton': 'sum', 'tonh': 'sum', 'ley_ag': 'mean', 'ley_fe': 'mean','ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'})
# df_reportes.groupby(['mining', 'year', 'month', 'date_extraction']).agg(ton=('ton', 'sum'), tonh=('tonh', 'sum'), ley_ag=('ley_ag', 'mean'), ley_fe=('ley_fe', 'mean'), ley_mn=('ley_mn', 'mean'), ley_pb=('ley_pb', 'mean'), ley_zn=('ley_zn', 'mean')).reset_index()
# mina = 'YUMPAG'
# mes = 'ENERO'
# # año = 2023
# df = df_dynamic.query('mining == @mina and month == @mes')

#####

# df_reportes.groupby([df_reportes['mining'], df_reportes['year'],df_reportes['month'], df_reportes['date_extraction']]).agg({'ton': 'sum', 'tonh': 'sum','ley_ag': 'mean', 'ley_fe': 'mean', 'ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'})
# df_reportes.groupby([df_reportes['mining'], df_reportes['year'],df_reportes['month'], df_reportes['nro_month'], df_reportes['week'],df_reportes['date_extraction']]).agg({'ton': 'sum', 'tonh': 'sum','ley_ag': 'mean', 'ley_fe': 'mean', 'ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'})

# UNIQUES

# year = df_reportes['year'].unique()
# month = df_reportes['month'].unique()
# status = df_reportes['status'].unique()
# mining = df_reportes['mining'].unique()
# ubication = df_reportes['ubication'].unique()
# turn = df_reportes['turn'].unique()
# level = df_reportes['level'].unique()
# type = df_reportes['type'].unique()
# veta = df_reportes['veta'].unique()
# tajo = df_reportes['tajo'].unique()
# dominio = df_reportes['dominio'].unique()
# columns = df_reportes.columns

columns = pd.DataFrame(df_main.dtypes, columns=['type'])
columns.reset_index(inplace=True)
columns.rename(columns={'index': 'name'}, inplace=True)
columns['type'] = columns['type'].astype(str)
columns = columns.to_dict('records')


array = ['mining', 'year', 'date']
word = 'month'
idx = [i for i, s in enumerate(array) if word in s]
nro_month = 'nro_month'
if len(idx) != 0:
    array.insert(idx[0], nro_month)
df = df_reportes.groupby(array).agg(ton=('ton', 'sum'), tonh=('tonh', 'sum'), ley_ag=('ley_ag', 'mean'), ley_fe=('ley_fe', 'mean'), ley_mn=('ley_mn', 'mean'), ley_pb=('ley_pb', 'mean'), ley_zn=('ley_zn', 'mean'), tmh_ag=('tmh_ag', 'sum'), tmh_fe=('tmh_fe', 'sum'), tmh_mn=('tmh_mn', 'sum'), tmh_pb=('tmh_pb', 'sum'), tmh_zn=('tmh_zn', 'sum')).reset_index()

#to dict
# df = df.to_dict('records')
# 1698814800
ts = '1704085200'
mining = 'YUMPAG'
date = datetime.fromtimestamp(int(ts)).strftime("%d/%m/%Y")
months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO','JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE','DICIEMBRE']
idxMonth = datetime.fromtimestamp(int(ts)).month - 1
month = months[idxMonth]
year = datetime.fromtimestamp(int(ts)).strftime('%Y')

df_main['tonh_ley_ag'] = df_main['tonh'] * df_main['ley_ag']
df_main['tonh_ley_fe'] = df_main['tonh'] * df_main['ley_fe']
df_main['tonh_ley_mn'] = df_main['tonh'] * df_main['ley_mn']
df_main['tonh_ley_pb'] = df_main['tonh'] * df_main['ley_pb']
df_main['tonh_ley_zn'] = df_main['tonh'] * df_main['ley_zn']
df1 = df_main.query('month == @month and year == @year and mining == @mining').groupby(['date']).agg({'tonh': 'sum', 'tonh_ley_ag': 'sum', 'tonh_ley_fe': 'sum', 'tonh_ley_mn': 'sum', 'tonh_ley_pb': 'sum', 'tonh_ley_zn': 'sum' }).reset_index()
df1['Ag'] = df1['tonh_ley_ag'] / df1['tonh']
df1['Fe'] = df1['tonh_ley_fe'] / df1['tonh']
df1['Mn'] = df1['tonh_ley_mn'] / df1['tonh']
df1['Pb'] = df1['tonh_ley_pb'] / df1['tonh']
df1['Zn'] = df1['tonh_ley_zn'] / df1['tonh']
# date to datetime firstday
df1['date'] = pd.to_datetime(df1['date'], format='%d/%m/%Y')
df2 = df_prog.query('mining == @mining and month == @month and year == @year')
if len(df2) == 0:
    df3 = df1.copy()
    df3['timestamp'] = df3['date'].apply(lambda x: datetime.strptime(x.strftime('%d/%m/%Y'), '%d/%m/%Y').timestamp())
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

df_cancha= df_reportes.query('status != "Planta"')
df_cancha.reset_index(inplace=True)
df_ruma = df_cancha.groupby(['ubication', 'mining', 'dominio', 'cod_tableta']).agg({'tonh': 'sum', 'tmh_ag': 'sum', 'tajo': ['unique']}).reset_index()
df_ruma.columns = ['ubication', 'mining', 'dominio', 'cod_tableta', 'tonh', 'tmh_ag', 'tajo']
df_ruma['ley_ag'] = df_ruma['tmh_ag'] / df_ruma['tonh']
ruma = df_ruma.to_dict('records')
# ruma2 = df_ruma.to_json(orient='records')
# df_ruma.to_json('ruma.json', orient='records')
# df_reportes.to_json('trip.json', orient='records')
# sort df_main
# df_main.sort_values(by=['timestamp'], inplace=True)
# df_main.to_json('main.json', orient='records')

# 1693544400 SEPTIEMBRE 2023
# 1696136400 OCTUBRE 2023 (OK)
# 1698814800 NOVIEMBRE 2023 (OK)
# 1701406800 DICIEMBRE 2023
# 1704085200 ENERO 2024
# 1698814800 NOVIEMBRE 2023

