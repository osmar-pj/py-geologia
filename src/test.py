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
trips = db['trips']
df_trips = pd.DataFrame(list(trips.find()))
df_trips['_id'] = df_trips['_id'].astype(str)
df_main= df_trips.query('statusTrip == "waitBeginDespacho" or statusTrip == "Despachando"').reset_index(drop=True)

def leyPonderada(x):
        return np.average(x, weights=df_main.loc[x.index, 'tonh'])

# df_main.dropna(subset=['ley_ag', 'ley_pb', 'ley_zn'], inplace=True)

df = df_main.groupby(['ubication', 'dominio']).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
_df = df_main.groupby(['ubication']).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

pointValues = {
        'vp_ag': 13,
        'vp_pb': 14.69,
        'vp_zn': 13.76,
}
def nsr(df):
        df['ag_rec'] = [0.28877 * x if x < 2.8 else 0.0422 * np.log(x) + 0.768505 for x in df['ley_ag']]
        df['pb_rec'] = [2.2829 * x if x < 0.4 else 0.0024 * x + 0.896 for x in df['ley_pb']]
        df['zn_rec'] = [0.81564 * x if x < 0.55 else 0.14627 * np.log(x) + 0.60619 if x < 7.85 else 0.808 for x in df['ley_zn']]
        df['nsr'] = df['ag_rec'] * pointValues['vp_ag'] * df['ley_ag'] + df['pb_rec'] * pointValues['vp_pb'] * df['ley_pb'] + df['zn_rec'] * pointValues['vp_zn'] * df['ley_zn']
        df['ag_eq'] = df['nsr'] / (pointValues['vp_ag'] * df['ag_rec'])
        return df

df = nsr(df)
_df = nsr(_df)
df['index'] = df.index
_df['index'] = _df.index