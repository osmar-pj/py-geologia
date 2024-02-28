import pandas as pd
import numpy as np
from datetime import datetime, timedelta

pd.set_option('display.max_columns', None)

df = pd.read_csv('../data/reporte_canchas.csv')
df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.year

df['month'] = df['date'].dt.month
months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
df['month'] = df['month'].apply(lambda x: months[x-1])
df['status'] = df['status'].replace('pLANTA', 'Planta')
df['status'] = df['status'].replace('PLANTA ', 'Planta')
df['status'] = df['status'].replace('PLANTA', 'Planta')
df['status'] = df['status'].replace('CANCHA', 'Cancha')
df['ubication'] = df['ubication'].replace('cancha 1', 'Cancha 1')
df['ubication'] = df['ubication'].replace('Cancha 3', 'Cancha Colquicocha')
df['turn'].fillna('-', inplace=True)
df['mining'] = df['zona']
# drop column zona
df = df.drop(columns=['zona'])
df['mining'] = df['mining'].replace('Yumpag', 'YUMPAG')
df['level'].fillna(3900, inplace=True)
df['type'] = df['type'].replace('AVANCE ', 'AVANCE')
df['veta'] = df['veta'].replace('Cachipampa', 'V_CACHIPAMPA')
df['veta'] = df['veta'].replace('Camila ', 'V_CAMILA')
df['veta'] = df['veta'].replace('CAMILA ', 'V_CAMILA')
df['veta'] = df['veta'].replace('Vanessa', 'V_VANESSA')
df['veta'] = df['veta'].replace('Cachipampa 5', 'V_CACHIPAMPA 5')
df['veta'] = df['veta'].replace('Cachipampa-5', 'V_CACHIPAMPA 5')
df['veta'] = df['veta'].replace('CACHIPAMPA 5', 'V_CACHIPAMPA 5')
df['veta'] = df['veta'].replace('Sonia Norte', 'V_SONIA NORTE')
df['veta'] = df['veta'].replace('SONIA NORTE', 'V_SONIA NORTE')
df['veta'] = df['veta'].replace('Lilia', 'V_LILIA')
df['veta'] = df['veta'].replace('GINA_', 'V_GINA')
df['veta'] = df['veta'].replace('GINA', 'V_GINA')
df['tajo'].fillna('TJ 6432', inplace=True)
df['vagones'].fillna(0, inplace=True)
df['cod_despacho'].fillna('-', inplace=True)
# remove ',' from ton column
df['ton'] = df['ton'].str.replace(',', '')
df['ton'] = df['ton'].astype(float)
df['ley_ag'].fillna(0, inplace=True)
df['ley_fe'].fillna(0, inplace=True)
df['ley_mn'].fillna(0, inplace=True)
df['ley_pb'].fillna(0, inplace=True)
df['ley_zn'].fillna(0, inplace=True)
df['tmh_ag'] = df['ley_ag'] * df['tonh']
df['tmh_fe'] = df['ley_fe'] * df['tonh']
df['tmh_mn'] = df['ley_mn'] * df['tonh']
df['tmh_pb'] = df['ley_pb'] * df['tonh']
df['tmh_zn'] = df['ley_zn'] * df['tonh']
df['cod_tableta'].fillna('-', inplace=True)
df['dateSupply'] = pd.to_datetime(df['dateSupply'])
# df['dateSupply'] = df['dateSupply'].replace(np.nan, None)
df['tajo'] = df['tajo'].replace(['Tj 6432', 'Tj  6432'], 'TJ 6432')
df['tajo'] = df['tajo'].replace(['Tj 6432-1', 'TJ6432-1'], 'TJ 6432-1')
df['tajo'] = df['tajo'].replace(['Tj  6609'], 'TJ 6609')
df['tajo'] = df['tajo'].replace(['Tj  6608', 'Tj 6608'], 'TJ 6608')
df['tajo'] = df['tajo'].replace(['Tj  6618'], 'TJ 6618')
df['tajo'] = df['tajo'].replace(['Tj 6408'], 'TJ 6408')
df['tajo'] = df['tajo'].replace(['Tj 6618'], 'TJ 6618')
df['tajo'] = df['tajo'].replace(['Tj 6431-1', 'Tj  6431-1'], 'TJ 6431-1')
df['tajo'] = df['tajo'].replace(['Tj 6488', 'Tj  6488', 'Tj6488'], 'TJ 6488')
df['tajo'] = df['tajo'].replace(['Tj 500-11p', 'Tj 500-11P'], 'TJ 500-11P')
df['tajo'] = df['tajo'].replace(['Tj 500-5P', 'Tj 500-5p'], 'TJ 500-5P')
df['tajo'] = df['tajo'].replace(['Tj 500-7P'], 'TJ 500-7P')
df['tajo'] = df['tajo'].replace(['Tj.500-3p'], 'TJ 500-3P')
df['tajo'] = df['tajo'].replace(['Sn6431-2'], 'SN 6431-2')
df['tajo'] = df['tajo'].replace(['Tj 300-3p'], 'TJ 300-3P')
df['tajo'] = df['tajo'].replace(['Tj 400-1p', 'Tj 400-1P'], 'TJ 400-1P')
df['tajo'] = df['tajo'].replace(['Tj 500-8P-S1'], 'TJ 500-8P-S1')
df.drop(['LEY CANCHA Ag2', 'LEY CANCHA Fe3', 'LEY CANCHA Mn4', 'LEY CANCHA Pb5', 'LEY CANCHA Zn6'], axis=1, inplace=True)

df['carriage'] = 'Camion'
# if vagones is different 0 then carriage is 'Vagon'
df.loc[df['vagones'] > 0, 'carriage'] = 'Vagones'

df.to_json('canchas.json', orient='records')

# def leyPonderada(x):
#         return np.average(x, weights=df_main.loc[x.index, 'tonh'])

# df_main.dropna(subset=['ley_ag', 'ley_pb', 'ley_zn'], inplace=True)

# df = df_main.groupby(['ubication', 'dominio']).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
# _df = df_main.groupby(['ubication']).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

# pointValues = {
#         'vp_ag': 13,
#         'vp_pb': 14.69,
#         'vp_zn': 13.76,
# }
# def nsr(df):
#         df['ag_rec'] = [0.28877 * x if x < 2.8 else 0.0422 * np.log(x) + 0.768505 for x in df['ley_ag']]
#         df['pb_rec'] = [2.2829 * x if x < 0.4 else 0.0024 * x + 0.896 for x in df['ley_pb']]
#         df['zn_rec'] = [0.81564 * x if x < 0.55 else 0.14627 * np.log(x) + 0.60619 if x < 7.85 else 0.808 for x in df['ley_zn']]
#         df['nsr'] = df['ag_rec'] * pointValues['vp_ag'] * df['ley_ag'] + df['pb_rec'] * pointValues['vp_pb'] * df['ley_pb'] + df['zn_rec'] * pointValues['vp_zn'] * df['ley_zn']
#         df['ag_eq'] = df['nsr'] / (pointValues['vp_ag'] * df['ag_rec'])
#         return df

# df = nsr(df)
# _df = nsr(_df)
# df['index'] = df.index
# _df['index'] = _df.index