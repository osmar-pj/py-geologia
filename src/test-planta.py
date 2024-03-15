from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
if not os.path.exists('assets'):
    os.makedirs('assets')
import pytz


from dotenv import load_dotenv
load_dotenv()
client = MongoClient(os.getenv('MONGO_URI'))

db = client['wapsi']
pd.set_option('display.max_columns', None)

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

# timestamp = 1625097600
ts = '1704085200'
# ts = '1709240066'
# ts = data['ts']
category = 'planta'
arr = ['zona', 'dominio']
mining = 'YUMPAG'

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
df1 = df_trips.query('month == @month and year == @year and mining == @mining').groupby(['date']).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

df1['date'] = pd.to_datetime(df1['date'], format='%d/%m/%Y')
df2 = df_prog.query('month == @month and year == @year and mining == @mining')

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

df = df3.copy()
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid")

fig, ax = plt.subplots(figsize=(10, 5))
sns.barplot(x='createdAt', y='tonh', data=df, ax=ax, color='green')

# from plotly.subplots import make_subplots
# import plotly.graph_objects as go
# import plotly.express as px
# import plotly.io as pio

# # dibujar twinx line ley_ag y ley_ag_prog y bar tonh
# fig = make_subplots(specs=[[{"secondary_y": True}]])
# fig.add_trace(go.Scatter(x=df['date'], y=df['ley_ag'], mode='lines', name='ley_ag', line=dict(color='blue')), secondary_y=False)
# fig.add_trace(go.Scatter(x=df['date'], y=df['ley_ag_prog'], mode='lines', name='ley_ag_prog', line=dict(color='red')), secondary_y=False)
# fig.add_trace(go.Bar(x=df['date'], y=df['tonh'], name='tonh', marker=dict(color='green')), secondary_y=True)

# fig.update_layout(title_text='Ley Ag y Toneladas', xaxis_title='Fecha', yaxis_title='Ley Ag', yaxis2_title='Toneladas')
# fig.update_xaxes(tickangle=45, tickformat='%d/%m/%Y')
# fig.update_yaxes(title_text='Ley Ag', secondary_y=False)
# fig.update_yaxes(title_text='Toneladas', secondary_y=True)
# fig.show()

# from fpdf import FPDF

# WIDTH = 210
# HEIGHT = 297
# pdf = FPDF()
# pdf.set_auto_page_break(auto=True, margin=15)
# pdf.add_page()
# pdf.set_font("Arial", size=12)
# pdf.cell(0, 10, "Reporte de Planta", 0, 1, 'C')

# # get .png file of sns to show graphic
# pdf.image('../assets/lineplot.png', 0, 40, WIDTH/2)
# # text aside the graphic
# pdf.set_xy(WIDTH, 40)

# pdf.output("report2.pdf")