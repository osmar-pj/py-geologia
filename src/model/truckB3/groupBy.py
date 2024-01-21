from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pytz


def byRuta(df_trips):
    df_ruta = df_trips[['ruta', 'weightTotal', 'mineral', 'desmonte']].groupby(['ruta']).agg({'weightTotal': ['count', 'sum'], 'mineral': 'sum', 'desmonte': 'sum'})
    df_ruta.columns = ['totalTrips', 'totalWeight', 'mineral', 'desmonte']
    df_ruta.reset_index(inplace=True)

    df_rutaTurn = df_trips[['ruta', 'turno', 'weightTotal']].groupby(['ruta', 'turno']).agg({'weightTotal': 'sum'})
    df_rutaTurn = df_rutaTurn.reset_index()
    df_rutaTurn = df_rutaTurn.pivot(index='ruta', columns='turno', values='weightTotal')
    df_rutaTurn = df_rutaTurn.reset_index()

    df_ruta = df_ruta.merge(df_rutaTurn, on=['ruta'], how='left')
    df_ruta.fillna(0, inplace=True)
    return df_ruta

def byTruck(df_trips):
    df_truckProduction = df_trips.groupby(['tag']).agg({'weightTotal': ['count', 'sum'], 'mineral': 'sum', 'desmonte': 'sum', 'timeInoperation': ['sum', 'mean']})
    df_truckProduction.columns = ['totalTrips', 'totalWeight', 'mineral', 'desmonte', 'timeInoperation', 'meanTime']
    df_truckProduction.reset_index(inplace=True)

    df_truckTurn = df_trips.groupby(['tag', 'turno']).agg({'weightTotal': 'sum'})
    df_truckTurn = df_truckTurn.reset_index()
    df_truckTurn = df_truckTurn.pivot(index='tag', columns='turno', values='weightTotal')
    df_truckTurn = df_truckTurn.reset_index()

    df_truckProduction = df_truckProduction.merge(df_truckTurn, on=['tag'], how='left')
    df_truckProduction.fillna(0, inplace=True)
    return df_truckProduction


def byTurnTruck(df):
    df_turno = df.groupby('turno').agg({'weightTotal': 'sum'})
    df_turno.columns = ['weightTotal']
    df_turno = df_turno.T
    return df_turno

def bestScoreTruck(df, c):
    _df = df.groupby([c]).agg({'weightTotal': 'sum', 'timeIda': 'mean', 'timeRetorno': 'mean', 'operationTruck_Id': 'count'})
    idx = _df['weightTotal'].idxmax()
    df_score = _df.loc[idx]
    df_score['periodo'] = idx
    return df_score

def toGraphicTruck(df, a, b):
    df_graphic = df.groupby([a, b]).agg({'mining': 'count', 'weightTotal': 'sum'})
    df_graphic.columns = ['totalTrips', 'weight']
    df_graphic.reset_index(inplace=True)
    df_graphic = df_graphic.pivot(index=b, columns=a, values='weight').T
    df_graphic = df_graphic.reset_index()
    df_graphic = df_graphic.fillna(0)
    return df_graphic

def getEventsTrucks(df):
    df_copy = df.copy()
    df_filter = df_copy.sort_values(by='createdAt', ascending=False)
    
    df_filter = df_filter[['name', 'tag', 'events', 'createdAt']]
    
    df_events = df_filter.loc[df_filter['events'].apply(lambda x: len(x) > 0)]
    
    return df_events

# REUTILIZABLE

def completeWeekTruck(df_graphic):
    df_graphic['datetime'] = pd.to_datetime(df_graphic['date'], format='%d/%m/%Y')
    last_date = df_graphic['datetime'].max()
    last_friday = last_date - timedelta(days=(last_date.weekday() + 3) % 7)
    week_dates = [last_friday + timedelta(days=i) for i in range(7)]
    week_dates_str = [date.strftime('%d/%m/%Y') for date in week_dates]
    missing_dates = [date for date in week_dates_str if date not in df_graphic['date'].values]
    if len(missing_dates) > 0:
        missing_rows = pd.DataFrame({'date': missing_dates, 'datetime': pd.to_datetime(missing_dates, format='%d/%m/%Y')})
        df_graphic = pd.concat([df_graphic, missing_rows], ignore_index=True)
    df_graphic = df_graphic.fillna(0)
    df_graphic['day'] = df_graphic['datetime'].dt.day_name()
    df_graphic['day'] = df_graphic['day'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa', 'Do'])
    df_graphic.fillna(0, inplace=True)
    df_graphic.sort_values(by=['datetime'], inplace=True)
    return df_graphic

def completeMonthTruck(df_graphic):
    df_graphic['datetime'] = pd.to_datetime(df_graphic['date'], format='%d/%m/%Y')
    fecha = df_graphic['datetime'].min()
    first_date = fecha.replace(day=1)
    siguiente_mes = fecha.replace(day=28) + timedelta(days=4)  # Asegurar que avanzamos al siguiente mes
    last_date = siguiente_mes - timedelta(days=siguiente_mes.day)
    rango = pd.date_range(start=first_date, end=last_date)

    month_dates = [first_date + timedelta(days=i) for i in range(len(rango))]
    month_dates_str = [date.strftime('%d/%m/%Y') for date in month_dates]
    missing_dates = [date for date in month_dates_str if date not in df_graphic['date'].values]
    if len(missing_dates) > 0:
        missing_rows = pd.DataFrame({'date': missing_dates, 'datetime': pd.to_datetime(missing_dates, format='%d/%m/%Y')})
        df_graphic = pd.concat([df_graphic, missing_rows], ignore_index=True)
    df_graphic = df_graphic.fillna(0)
    df_graphic['day'] = df_graphic['datetime'].dt.day_name()
    df_graphic['day'] = df_graphic['day'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa', 'Do'])
    df_graphic.fillna(0, inplace=True)
    df_graphic.sort_values(by=['datetime'], inplace=True)
    return df_graphic