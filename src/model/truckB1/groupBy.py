from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
from model.truck.trips import getTripsTruck

import pandas as pd
import os
import pytz

load_dotenv()

client = MongoClient(os.getenv('MONGO_URI'))

db = client['wapsi']
pd.set_option('display.max_columns', None)

def toGraphicTruck(df_trips):
    df_graphic = df_trips.query('place == "echadero"').groupby(['date', 'turno']).count().reset_index() 
    df_graphic = df_graphic.pivot(index='turno', columns='date', values='tag').T
    df_graphic = df_graphic.reset_index()
    df_graphic = df_graphic.fillna(0)
    df_graphic['date'] = pd.to_datetime(df_graphic['date'], format='%d/%m/%Y')
    df_graphic['day'] = df_graphic['date'].dt.day_name()
    df_graphic['day'] = df_graphic['day'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa', 'Do'])

    df_trucks = (df_trips[['tag', 'date', 'turno', 'area', 'time']].groupby(['tag', 'date', 'turno', 'area']).sum().groupby(['tag', 'area']).mean() / 60).reset_index()
    return df_graphic

def byTurn(df_trips):
    df_turno = df_trips.groupby('turno').agg({'weight': 'sum'})
    df_turno.columns = ['weight']
    df_turno = df_turno.T
    return df_turno