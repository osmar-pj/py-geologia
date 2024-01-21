# Osmar Palomino 28-11-2023
# Funciones de agrupacion para distintos parametros

from datetime import datetime, timedelta
import pandas as pd
import pytz

def byPique(df_trips):
    # df_tripsWeek = df_trips.query('week == @week')
    df_pique = df_trips[['pique', 'polimetalico', 'desmonte', 'alabandita', 'carbonato', 'totalValidWagons', 'G1', 'G2', 'G3', 'G4']].groupby(['pique']).agg({'totalValidWagons': ['count', 'sum'], 'polimetalico': 'sum', 'desmonte': 'sum', 'alabandita': 'sum', 'carbonato': 'sum', 'G1': 'sum', 'G2': 'sum', 'G3': 'sum', 'G4': 'sum'})
    df_pique.columns = ['totalTrips', 'totalWeight', 'polimetalico', 'desmonte', 'alabandita', 'carbonato', 'G1', 'G2', 'G3', 'G4']
    df_pique.reset_index(inplace=True)

    df_piqueTurn = df_trips[['pique', 'turno', 'totalValidWagons']].groupby(['pique', 'turno']).agg({'totalValidWagons': 'sum'})
    df_piqueTurn = df_piqueTurn.reset_index()
    df_piqueTurn = df_piqueTurn.pivot(index='pique', columns='turno', values='totalValidWagons')
    df_piqueTurn = df_piqueTurn.reset_index()

    df_pique = df_pique.merge(df_piqueTurn, on=['pique'], how='left')
    df_pique.fillna(0, inplace=True)
    return df_pique

def byLocomotive(df_trips):
    df_locomProduction = df_trips.groupby(['tag']).agg({'totalValidWagons': ['count', 'sum'], 'polimetalico': 'sum', 'desmonte': 'sum', 'alabandita': 'sum', 'carbonato': 'sum', 'G1': 'sum', 'G2': 'sum', 'G3': 'sum', 'G4': 'sum', 'timeInoperation': ['sum', 'mean']})
    df_locomProduction.columns = ['totalTrips', 'totalWeight', 'polimetalico', 'desmonte', 'alabandita', 'carbonato', 'G1', 'G2', 'G3', 'G4', 'timeInoperation', 'meanTime']
    df_locomProduction.reset_index(inplace=True)

    df_locomTurn = df_trips.groupby(['tag', 'turno']).agg({'totalValidWagons': 'sum'})
    df_locomTurn = df_locomTurn.reset_index()
    df_locomTurn = df_locomTurn.pivot(index='tag', columns='turno', values='totalValidWagons')
    df_locomTurn = df_locomTurn.reset_index()
    
    df_locomProduction = df_locomProduction.merge(df_locomTurn, on=['tag'], how='left')
    df_locomProduction.fillna(0, inplace=True)
    return df_locomProduction

def getWeekByTurn(week, df_trips):
    df_tripsWeek = df_trips.query('week == @week')
    df_stat = df_tripsWeek.groupby(['date', 'turno']).agg({'mining': 'count', 'totalValidWagons': 'sum'})
    df_stat.columns = ['totalTrips', 'totalValidWagons']
    df_stat.reset_index(inplace=True)
    df_stat = df_stat.pivot(index='turno', columns='date', values='totalValidWagons').T
    df_stat = df_stat.reset_index()
    df_stat['datetime'] = pd.to_datetime(df_stat['date'], format='%d/%m/%Y')
    last_date = df_stat['datetime'].max()
    last_friday = last_date - timedelta(days=(last_date.weekday() + 3) % 7)
    week_dates = [last_friday + timedelta(days=i) for i in range(7)]
    week_dates_str = [date.strftime('%d/%m/%Y') for date in week_dates]
    missing_dates = [date for date in week_dates_str if date not in df_stat['date'].values]
    if len(missing_dates) > 0:
        missing_rows = pd.DataFrame({'date': missing_dates, 'datetime': pd.to_datetime(missing_dates, format='%d/%m/%Y')})
        df_stat = pd.concat([df_stat, missing_rows], ignore_index=True)
    df_stat = df_stat.fillna(0)
    df_stat['day'] = df_stat['datetime'].dt.day_name()
    df_stat['day'] = df_stat['day'].replace(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ['Lu', 'Ma', 'Mi', 'Ju', 'Vi', 'Sa', 'Do'])
    df_stat.fillna(0, inplace=True)
    df_stat.sort_values(by=['datetime'], inplace=True)
    return df_stat

def byTurn(df):
    df_turno = df.groupby('turno').agg({'totalValidWagons': 'sum'})
    df_turno.columns = ['totalValidWagons']
    df_turno = df_turno.T
    return df_turno

def bestScore(df, c):
    _df = df.groupby([c]).agg({'weight': 'sum', 'timeIda': 'mean', 'timeRetorno': 'mean', 'operation_Id': 'count'})
    idx = _df['weight'].idxmax()
    df_score = _df.loc[idx]
    df_score['periodo'] = idx
    return df_score

def toGraphic(df, a, b):
    df_graphic = df.groupby([a, b]).agg({'mining': 'count', 'totalValidWagons': 'sum'})
    df_graphic.columns = ['totalTrips', 'totalValidWagons']
    df_graphic.reset_index(inplace=True)
    df_graphic = df_graphic.pivot(index=b, columns=a, values='totalValidWagons').T
    df_graphic = df_graphic.reset_index()
    df_graphic = df_graphic.fillna(0)
    return df_graphic

# RECICLAR CODIGO

def completeWeek(df_graphic):
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

def completeMonth(df_graphic):
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