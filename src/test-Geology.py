from flask import Flask, jsonify, request, Response
from flask_pymongo import PyMongo
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

df_reportes.rename(columns={'MES': 'month', 'FECHA EXTRACCION': 'date_extraction', 'Año': 'year', 'ESTADO': 'status', 'UBICACIÓN': 'ubication', 'TURNO': 'turn', 'ZONA': 'mining', 'EMPRESA': 'company', 'NIVEL': 'level', 'TIPO': 'type', 'Veta': 'veta', 'Tajo': 'tajo', 'Dominio': 'dominio', 't': 'ton', 'TMH': 'tonh', 'N° carros': 'vagones', 'Ley Stope': 'ley_stope', 'CODIGO MUESTRA': 'cod_muestra', 'LEY SO': 'ley_so', 'LEY CANCHA Ag': 'ley_ag', 'LEY CANCHA Fe': 'ley_fe', 'LEY CANCHA Mn': 'ley_mn', 'LEY CANCHA Pb': 'ley_pb', 'LEY CANCHA Zn': 'ley_zn', 'Ley Original': 'ley_original', 'Ley cancha 2': 'ley_cancha2', 'Codigo Muestra 3': 'cod_muestra3', 'Ley cancha 3': 'ley_cancha3', 'LEY CANCHA castigada': 'ley_cancha_castigada', 'LEY inicial': 'ley_inicial', 'Rango': 'rango', 'Ag*TMH': 'tmh_ag', 'Fe*TMH': 'tmh_fe', 'Mn*TMH': 'tmh_mn', 'Pb*TMH': 'tmh_pb', 'Zn*TMH': 'tmh_zn', 'Codigo tableta': 'cod_tableta', 'Labor Rep.': 'labor', 'FECHA ABASTECIMIENTO': 'date_abas', 'Columna1': 'columna'}, inplace=True)

df_reportes['mining'] = df_reportes['mining'].replace('Yumpag', 'YUMPAG')

df_reportes['turn'].fillna('SIN TURNO', inplace=True)
df_reportes['level'].fillna(0, inplace=True)

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

df_reportes['date_extraction'] = pd.to_datetime(df_reportes['date_extraction']).dt.strftime('%d/%m/%Y')

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

df_reportes['week'] = pd.to_datetime(df_reportes['date_extraction'], format='%d/%m/%Y').dt.isocalendar().week
df_reportes['nro_month'] = df_reportes['month'].replace({'ENERO': 1, 'FEBRERO': 2, 'MARZO': 3, 'ABRIL': 4, 'MAYO': 5, 'JUNIO': 6, 'JULIO': 7, 'AGOSTO': 8, 'SEPTIEMBRE': 9, 'OCTUBRE': 10, 'NOVIEMBRE': 11, 'DICIEMBRE': 12})
#concatenar el nro de mes con el año
df_reportes['nro_month'] = df_reportes['nro_month'].astype(str)

df_main = df_reportes.copy()
df_main.drop(['columna', 'company', 'cod_muestra', 'vagones', 'cod_muestra3', 'ley_cancha_castigada', 'ley_stope', 'ley_inicial', 'tmh_ag', 'tmh_fe', 'tmh_mn', 'tmh_pb', 'tmh_zn', 'labor', 'ley_original', 'ley_cancha2', 'ley_cancha3', 'cod_tableta'], axis=1, inplace=True)

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

array = ['year', 'month', 'mining', 'date_extraction']
df = df_reportes.groupby(array).agg(ton=('ton', 'sum'), tonh=('tonh', 'sum'), ley_ag=('ley_ag', 'mean'), ley_fe=('ley_fe', 'mean'), ley_mn=('ley_mn', 'mean'), ley_pb=('ley_pb', 'mean'), ley_zn=('ley_zn', 'mean')).reset_index()

#to dict
df = df.to_dict('records')