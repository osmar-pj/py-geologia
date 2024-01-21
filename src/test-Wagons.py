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


def get_week_number(date=None):
    if date is None:
        date = datetime.now()
    year = date.year
    year_start = datetime(year, 1, 1, 19, 0, 0)  # 1 de enero a las 19:00 AM
    actual_year = year

    # Establecer el inicio del año al primer jueves a las 7:00 AM
    year_start += timedelta(days=(2 - year_start.weekday() + 7) % 7)

    if year_start > date:
        year_start = datetime(year - 1, 1, 1, 19, 0, 0)
        year_start += timedelta(days=(3 - year_start.weekday() + 7) % 7)
        actual_year -= 1

    diff_days = (date - year_start).days
    week_number = diff_days // 7 + 1
    # return week_number
    return f"{week_number}-{actual_year}"


def getOperationsWagon():
    operations = db['operations']
    df_operations = pd.DataFrame(list(operations.find()))

    locomotives = db['locomotives']
    df_locomotives = pd.DataFrame(list(locomotives.find()))
    df_locomotives['locomotive_Id'] = df_locomotives['locomotiveId']

    operators = db['operators']
    df_operators = pd.DataFrame(list(operators.find()))
    df_operators['operator_Id'] = df_operators['operatorId']

    # df_operations = df_operations.drop(['_id'], axis=1)
    df_operations['_id'] = df_operations['_id'].astype(str)

    # df_operations remove repeat values
    df_operations = df_operations.drop_duplicates(subset=['createdAt'], keep='last')
    df_operations.reset_index(drop=True, inplace=True)
    df = df_operations.copy()
    df['qtyCheckListVerifyL'] = df_operations['checklist_verifyL'].apply(lambda x: len([w for w in x if w['status'] == 'MAL']))
    df['qtyCheckListConditionsL'] = df_operations['checklist_conditionsL'].apply(lambda x: len([w for w in x if w['status'] == 'MAL']))
    df['qtyCheckListVerifyW'] = df_operations['checklist_verifyW'].apply(lambda x: len([w for w in x if w['status'] == 'MAL']))
    df['qtyCheckListConditionsW'] = df_operations['checklist_conditionsW'].apply(lambda x: len([w for w in x if w['status'] == 'MAL']))
    df['datetimeMina'] = df['createdAt'].dt.tz_localize('UTC').dt.tz_convert('America/Lima') + pd.Timedelta(hours=5)
    df['date'] = df['datetimeMina'].dt.strftime('%d/%m/%Y')
    df = df.drop(['checklist_verifyL', 'checklist_conditionsL', 'checklist_verifyW', 'checklist_conditionsW'], axis=1)
    df_operations = df_operations.merge(df_operators[['name', 'operator_Id']], on=['operator_Id'], how='left')
    df_operations = df_operations.merge(df_locomotives[['tag', 'locomotive_Id']], on=['locomotive_Id'], how='left')

    return df, df_operations


# GET TRIPS WAGONS

locomotives = db['locomotives']
df_locomotives = pd.DataFrame(list(locomotives.find()))
df_locomotives['locomotive_Id'] = df_locomotives['locomotiveId']

operators = db['operators']
df_operators = pd.DataFrame(list(operators.find()))
df_operators['operator_Id'] = df_operators['operatorId']

travels = db['travels']
df_travels = pd.DataFrame(list(travels.find()))

# df_travels = df_travels.drop(['_id'], axis=1)
# df_operators = df_operators.drop(['_id'], axis=1)
# df_locomotives = df_locomotives.drop(['_id'], axis=1)
df_travels['_id'] = df_travels['_id'].astype(str)
df_operators['_id'] = df_operators['_id'].astype(str)
df_locomotives['_id'] = df_locomotives['_id'].astype(str)

# now = datetime.now()
# days_to_subtract = (now.weekday() - 3) % 7  # 3 representa el índice del jueves en la semana
# start = now - timedelta(days=days_to_subtract, hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
# start = start.replace(hour=19, minute=0, second=0, microsecond=0)
# end = now
# # start to time UTC
# start = start.astimezone(pytz.utc)
# end = end.astimezone(pytz.utc)

df_travels = df_travels.drop_duplicates(subset=['createdAt', 'tourStart', 'tourEnd'], keep='last')
df_travels.reset_index(drop=True, inplace=True)
# eliminar columnas
df_travels = df_travels.drop(['status', 'travel_Id'], axis=1)
df_travels.dropna(inplace=True)

df_travels = df_travels[df_travels['operator_Id'] != 99]

df_travels['emptyTime'] = (df_travels['loadStart'] - df_travels['tourStart']) / 60000
df_travels['loadTime'] = (df_travels['loadEnd'] - df_travels['loadStart']) / 60000
df_travels['fullTime'] = (df_travels['tourEnd'] - df_travels['loadEnd']) / 60000
df_travels['downloadTime'] = (df_travels['downloadEnd'] - df_travels['downloadStart']) / 60000
df_travels['start'] = df_travels['tourStart']
df_travels['end'] = df_travels['downloadEnd']
df_travels['datetimeStart'] = pd.to_datetime(df_travels['start'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
df_travels['datetimeEnd'] = pd.to_datetime(df_travels['end'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
# df_travels['time'] = (df_travels['end'] - df_travels['start']) / 60000

# df_travels['datetimeTourStart'] = pd.to_datetime(df_travels['tourStart'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
# df_travels['datetimeTourEnd'] = pd.to_datetime(df_travels['tourEnd'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
# df_travels['datetimeLoadStart'] = pd.to_datetime(df_travels['loadStart'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
# df_travels['datetimeLoadEnd'] = pd.to_datetime(df_travels['loadEnd'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
# df_travels['datetimeDownloadStart'] = pd.to_datetime(df_travels['downloadStart'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
# df_travels['datetimeDownloadEnd'] = pd.to_datetime(df_travels['downloadEnd'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')

df_trips = df_travels.copy()

df_trips['nro_wagons'] = df_trips['wagons'].map(lambda x: len(x))
df_trips['desmonte'] = df_trips['wagons'].map(lambda x: len([w for w in x if w['material'] == 'DESMONTE']))
df_trips['polimetalico'] = df_trips['wagons'].map(lambda x: len([w for w in x if w['material'] == 'POLIMETALICO']))
df_trips['alabandita'] = df_trips['wagons'].map(lambda x: len([w for w in x if w['material'] == 'ALABANDITA']))
df_trips['carbonato'] = df_trips['wagons'].map(lambda x: len([w for w in x if w['material'] == 'CARBONATO']))
df_trips['mineral'] = df_trips['polimetalico'] + df_trips['alabandita'] + df_trips['carbonato']
df_trips['inoperativo'] = df_trips['wagons'].map(lambda x: len([w for w in x if w['material'] == 'INOPERATIVO']))
df_trips['vacio'] = df_trips['wagons'].map(lambda x: len([w for w in x if w['material'] == 'VACIO']))
df_trips['totalGibas'] = df_trips['destino'].map(lambda x: len(x))
df_trips['totalValidWagons'] = df_trips['polimetalico'] + df_trips['desmonte'] + df_trips['alabandita'] + df_trips['carbonato']
df_trips['G1'] = df_trips['destino'].map(lambda x: len([w for w in x if w == 'G1'])) * (df_trips['totalValidWagons'] / df_trips['totalGibas'])
df_trips['G2'] = df_trips['destino'].map(lambda x: len([w for w in x if w == 'G2'])) * (df_trips['totalValidWagons'] / df_trips['totalGibas'])
df_trips['G3'] = df_trips['destino'].map(lambda x: len([w for w in x if w == 'G3'])) * (df_trips['totalValidWagons'] / df_trips['totalGibas'])
df_trips['G4'] = df_trips['destino'].map(lambda x: len([w for w in x if w == 'G4'])) * (df_trips['totalValidWagons'] / df_trips['totalGibas'])
df_trips['weight'] = (df_trips['totalValidWagons']) * 8

# Agregando y clasificando por periodo mes, semana, dia
df_travels['datetimeTZ'] = df_travels['createdAt'].dt.tz_localize('UTC').dt.tz_convert('America/Lima')
df_travels['datetimeMina'] = df_travels['createdAt'].dt.tz_localize('UTC').dt.tz_convert('America/Lima') + pd.Timedelta(hours=5)
df_travels['date'] = df_travels['datetimeMina'].dt.strftime('%d/%m/%Y')
df_trips['date'] = df_travels['date']
df_trips['datetimeMina'] = df_travels['datetimeMina']
df_trips['datetimeTZ'] = df_travels['datetimeTZ']

df_trips['week'] = (df_trips['createdAt'] - pd.Timedelta(hours=5)).apply(lambda x: get_week_number(x))

# df_trips['nro_month'] = df_trips['datetimeMina'].dt.month

df_trips['nro_month'] = df_trips['datetimeMina'].dt.strftime('%m/%Y')
df_trips['month'] = df_trips['datetimeMina'].dt.strftime('%B')


# df_trips['month'] = df_trips['nro_month'].apply(lambda x: datetime(2023, x, 1).strftime('%B'))
df_trips['month'] = df_trips['month'].replace({'January': 'Ene', 'February': 'Feb', 'March': 'Mar', 'April': 'Abr', 'May': 'May', 'June': 'Jun', 'July': 'Jul', 'August': 'Ago', 'September': 'Sep', 'October': 'Oct', 'November': 'Nov', 'December': 'Dic'})
df_trips['hhmm'] = df_trips['datetimeTZ'].dt.strftime('%H:%M')
df_trips['hour'] = df_trips['datetimeTZ'].dt.hour

df_trips['timeActivityI'] = df_trips['activityI'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeActivityF'] = df_trips['activityF'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeDelayI'] = df_trips['delayI'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeDelayF'] = df_trips['delayF'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeStopI'] = df_trips['stopI'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeStopF'] = df_trips['stopF'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeInoperation'] = (df_trips['timeActivityI'] + df_trips['timeActivityF'] + df_trips['timeDelayI'] + df_trips['timeDelayF'] + df_trips['timeStopI'] + df_trips['timeStopF']) / 60
df_trips = df_trips.drop(['destino', 'wagons', 'wagonsI', 'wagonsF', 'activityI', 'activityF', 'delayI', 'delayF', 'stopI', 'stopF', 'datetimeStart', 'datetimeEnd'], axis=1)

df_resume, df1 = getOperationsWagon()
df_trips = df_trips.merge(df_resume[['operation_Id', 'locomotive_Id', 'operator_Id', 'mining', 'date', 'turno']], on=['operation_Id', 'locomotive_Id', 'operator_Id', 'date', 'mining'], how='left')
df_trips.dropna(subset=['turno'], inplace=True)
df_trips = df_trips.merge(df_operators[['name', 'operator_Id']], on=['operator_Id'], how='left')
df_trips = df_trips.merge(df_locomotives[['tag', 'locomotive_Id']], on=['locomotive_Id'], how='left')

df = pd.DataFrame()
for i, r in df_resume.iterrows():
    # filter by each operation in df_tripsWagon
    operation = r['operation_Id']
    operator = r['operator_Id']
    locomotive = r['locomotive_Id']
    mining = r['mining']
    date = r['date']
    _df = df_trips.query('operation_Id == @operation and operator_Id == @operator and locomotive_Id == @locomotive and mining == @mining and date == @date').copy()
    # if _df is empty continue
    # evaluar y numerar los viajes
    _df.loc[:, 'shift'] = _df['tourStart'].shift(-1)
    _df['timeIda'] = (_df['shift'] - _df['downloadEnd']) / 60000 # Tiempo en minutos que regresa vacio
    df = pd.concat([df, _df])
    
df['timeRetorno'] = (df['downloadEnd'] - df['tourStart']) / 60000 # Tiempo en minutos que regresa lleno
df.fillna(0, inplace=True)

# VIAJES GEOLOGIA

# df_tripsWagon = df.copy()
# df_tripsWagon['status'] = 'completo'
# df_tripsWagon['valid'] = np.where(df_tripsWagon['status'] == 'completo', 1, 0)
# df_tripsWagon['weightTotalTMS'] = (df_tripsWagon['weight'] * 0.94).round(1)
# df_tripsWagon['material'] = np.select([(df['mineral'] > 0) & (df['desmonte'] == 0), (df['desmonte'] > 0) & (df['mineral'] == 0), (df['mineral'] > 0) & (df['desmonte'] > 0)], ['MINERAL', 'DESMONTE', 'MIXTO'], default='OTRO')
# df_tripsWagon = df_tripsWagon[['_id', 'date', 'turno', 'name', 'tag', 'mining', 'totalValidWagons', 'weight', 'weightTotalTMS', 'material', 'createdAt']]
# df_tripsWagon.rename(columns={'_id': 'travel_Id', 'date': 'fecha', 'turno': 'turno', 'name': 'operador', 'tag': 'vehiculo', 'mining': 'mina', 'totalValidWagons': 'vagones','weight': 'ton', 'weightTotalTMS': 'tonh', 'material': 'material','createdAt': 'datetime'}, inplace=True)
