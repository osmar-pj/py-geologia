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

def getOperationTruck():
    truckOperations = db['truckoperations']
    df_truckOperations = pd.DataFrame(list(truckOperations.find()))

    drivers = db['drivers']
    df_drivers = pd.DataFrame(list(drivers.find()))
    df_drivers['driver_Id'] = df_drivers['driverId']

    trucks = db['trucks']
    df_trucks = pd.DataFrame(list(trucks.find()))
    df_trucks['truck_Id'] = df_trucks['truckId']

    df_truckOperations['_id'] = df_truckOperations['_id'].astype(str)

    df_truckOperations = df_truckOperations.drop_duplicates(subset=['createdAt'], keep='last')
    df_truckOperations.reset_index(drop=True, inplace=True)

    df = df_truckOperations.copy()
    df['datetimeMina'] = df['createdAt'].dt.tz_localize('UTC').dt.tz_convert('America/Lima') + pd.Timedelta(hours=5)
    df['date'] = df['datetimeMina'].dt.strftime('%d/%m/%Y')

    df_truckOperations = df_truckOperations.merge(df_drivers[['name', 'driver_Id']], on=['driver_Id'], how='left')
    df_truckOperations = df_truckOperations.merge(df_trucks[['tag', 'truck_Id']], on=['truck_Id'], how='left')

    return df, df_truckOperations


##### GET TRIPS TRUCKS #####

drivers = db['drivers']
df_drivers = pd.DataFrame(list(drivers.find()))
df_drivers['driver_Id'] = df_drivers['driverId']

trucks = db['trucks']
df_trucks = pd.DataFrame(list(trucks.find()))
df_trucks['truck_Id'] = df_trucks['truckId']

truckTravels = db['trucktravels']
df_truckTravels = pd.DataFrame(list(truckTravels.find()))

df_drivers['_id'] = df_drivers['_id'].astype(str)
df_trucks['_id'] = df_trucks['_id'].astype(str)
df_truckTravels['_id'] = df_truckTravels['_id'].astype(str)

df_truckTravels = df_truckTravels.drop_duplicates(subset=['createdAt'], keep='last')
df_truckTravels.reset_index(drop=True, inplace=True)
# df_truckTravels.dropna(inplace=True)

# EXCEPTUAR AL CONDUCTOR CON ID 1
df_truckTravels = df_truckTravels[df_truckTravels['driver_Id'] != 1]

df_truckTravels['loadTime'] = (df_truckTravels['loadEnd'] - df_truckTravels['loadStart']) / 60000
df_truckTravels['downloadTime'] = (df_truckTravels['downloadEnd'] - df_truckTravels['downloadStart']) / 60000

# df_truckTravels['emptyTime'] = (df_truckTravels['download'] - df_truckTravels['loadStart']) / 60000
df_truckTravels['fullTime'] = (df_truckTravels['downloadStart'] - df_truckTravels['loadEnd']) / 60000

df_truckTravels['start'] = df_truckTravels['loadStart']
df_truckTravels['end'] = df_truckTravels['downloadEnd']

df_truckTravels['datetimeStart'] = pd.to_datetime(df_truckTravels['start'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')
df_truckTravels['datetimeEnd'] = pd.to_datetime(df_truckTravels['end'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Lima')


df_truckTravels['events'] = df_truckTravels.apply(lambda row: [{'name': event['name'], 'time': event['time']} for col, events in row.items() if isinstance(events, list) for event in events if isinstance(event, dict) and event['time'] > 0], axis=1)


df_trips = df_truckTravels.copy()

df_truckTravels['datetimeTZ'] = df_truckTravels['createdAt'].dt.tz_localize('UTC').dt.tz_convert('America/Lima')
df_truckTravels['datetimeMina'] = df_truckTravels['createdAt'].dt.tz_localize('UTC').dt.tz_convert('America/Lima') + pd.Timedelta(hours=5)
df_truckTravels['date'] = df_truckTravels['datetimeMina'].dt.strftime('%d/%m/%Y')


df_trips['date'] = df_truckTravels['date']
df_trips['datetimeMina'] = df_truckTravels['datetimeMina']
df_trips['datetimeTZ'] = df_truckTravels['datetimeTZ']
df_trips['week'] = (df_trips['createdAt'] - pd.Timedelta(hours=5)).apply(lambda x: get_week_number(x))
df_trips['nro_month'] = df_trips['datetimeMina'].dt.strftime('%m/%Y')

df_trips['month'] = df_trips['datetimeMina'].dt.strftime('%B')
df_trips['month'] = df_trips['month'].replace({'January': 'Ene', 'February': 'Feb', 'March': 'Mar', 'April': 'Abr', 'May': 'May', 'June': 'Jun', 'July': 'Jul', 'August': 'Ago', 'September': 'Sep', 'October': 'Oct', 'November': 'Nov', 'December': 'Dic'})
df_trips['hhmm'] = df_trips['datetimeTZ'].dt.strftime('%H:%M')
df_trips['hour'] = df_trips['datetimeTZ'].dt.hour

df_trips['timeStop'] = df_trips['stop'].apply(lambda x: [a['time'] if isinstance(a, dict) and 'time' in a else '0' for a in x] if isinstance(x, list) else ['0']).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeStand'] = df_trips['stand'].apply(lambda x: [a['time'] if isinstance(a, dict) and 'time' in a else '0' for a in x] if isinstance(x, list) else ['0']).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeDelay'] = df_trips['delay'].apply(lambda x: [a['time'] if isinstance(a, dict) and 'time' in a else '0' for a in x] if isinstance(x, list) else ['0']).apply(lambda x: sum([int(a) for a in x]))
df_trips['timeMaintenance'] = df_trips['maintenance'].apply(lambda x: [a['time'] if isinstance(a, dict) and 'time' in a else '0' for a in x] if isinstance(x, list) else ['0']).apply(lambda x: sum([int(a) for a in x]))


# df_trips['timeStop'] = df_trips['stop'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
# df_trips['timeStand'] = df_trips['stand'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
# df_trips['timeDelay'] = df_trips['delay'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))
# df_trips['timeMaintenance'] = df_trips['maintenance'].apply(lambda x: [a['time'] for a in x]).apply(lambda x: ['0' if a == '' or a == None else a for a in x]).apply(lambda x: sum([int(a) for a in x]))

df_trips['timeInoperation'] = (df_trips['timeStop'] + df_trips['timeStand'] + df_trips['timeDelay'] + df_trips['timeMaintenance']) / 60
df_trips = df_trips.drop(['stop', 'stand', 'delay', 'maintenance', 'datetimeStart', 'datetimeEnd'], axis=1)

df_resume, df1 = getOperationTruck()
df_trips = df_trips.merge(df_resume[['operationTruck_Id', 'driver_Id', 'truck_Id', 'mining', 'ruta', 'date', 'turno']], on=['operationTruck_Id', 'driver_Id', 'truck_Id', 'mining', 'ruta', 'date'], how='left')
df_trips.dropna(subset=['turno'], inplace=True)
df_trips = df_trips.merge(df_drivers[['name', 'driver_Id']], on=['driver_Id'], how='left')
df_trips = df_trips.merge(df_trucks[['tag', 'truck_Id']], on=['truck_Id'], how='left')

df_trips = df_trips.drop_duplicates(subset=['createdAt'], keep='last')
df_trips.reset_index(drop=True, inplace=True)

# df_trips = df_trips.drop_duplicates(subset=['_id'], keep='last')
# df_trips.reset_index(drop=True, inplace=True)


df = pd.DataFrame()
for i, r in df_resume.iterrows():
    truckoperation = r['operationTruck_Id']
    driver = r['driver_Id']
    truck = r['truck_Id']
    mining = r['mining']
    ruta = r['ruta']
    date = r['date']

    _df = df_trips.query('operationTruck_Id == @truckoperation and driver_Id == @driver and truck_Id == @truck and mining == @mining and ruta == @ruta and date == @date').copy()

    _df.loc[:, 'shift'] = _df['loadStart'].shift(-1)

    _df['timeIda'] = (_df['shift'] - _df['downloadEnd']) / 60000

    df = pd.concat([df, _df])

df['timeRetorno'] = (df['downloadEnd'] - df['loadStart']) / 60000
df.fillna(0, inplace=True)
# df['weight'] = df.apply(lambda row: row['weight_gross'] / 1000 if 'YUM CARGUIO INTERIOR MINA - YUM CANCHA SUPERFICIE' not in row['ruta'] else row['weight_gross'], axis=1)
df['weight'] = df.apply(lambda row: round(row['weight_net'] / 1000, 1) if 'YUM CARGUIO INTERIOR MINA - YUM CANCHA SUPERFICIE' not in row['ruta'] else row['weight_net'], axis=1)


df['mineral'] = np.where(df['dominio'] == 'MINERAL', df['weight'], 0)
df['desmonte'] = np.where(df['dominio'] == 'DESMONTE', df['weight'], 0)

# df['mineral'] = np.where(df['dominio'] == 'MINERAL', df['weight'].astype(float), 0.0)
# df['desmonte'] = np.where(df['dominio'] == 'DESMONTE', df['weight'].astype(float), 0.0)

df['weightTotal'] = df['mineral'] + df['desmonte']

# obtener los viajes que tengan status completo
# df = df[df['status'] == 'incompleto']


# VIAJES GEOLOGIA

# df_tripsTruck = df.query('ruta == "YUM CANCHA SUPERFICIE - UCH CANCHA COLQUICOCHA" or ruta == "YUM CARGUIO INTERIOR MINA - UCH CANCHA COLQUICOCHA" ')
# df_tripsTruck['weightTotalTMS'] = (df_tripsTruck['weightTotal'] * 0.94).round(1)
# df_tripsTruck['material'] = np.select([(df_tripsTruck['mineral'] > 0) & (df_tripsTruck['desmonte'] == 0), (df_tripsTruck['desmonte'] > 0) & (df_tripsTruck['mineral'] == 0), (df_tripsTruck['mineral'] > 0) & (df_tripsTruck['desmonte'] > 0)], ['MINERAL', 'DESMONTE', 'MIXTO'], default='OTRO')
# df_tripsTruck = df_tripsTruck[['_id', 'date', 'turno', 'name', 'tag', 'mining', 'type', 'tajo', 'weightTotal', 'weightTotalTMS', 'material', 'status', 'valid', 'createdAt']]
# df_tripsTruck = df_tripsTruck[['_id', 'date', 'turno', 'name', 'tag', 'mining', 'type', 'tajo', 'weightTotal', 'weightTotalTMS', 'material', 'createdAt']]
# df_tripsTruck.rename(columns={'_id': 'travel_Id', 'date': 'fecha', 'turno': 'turno', 'name': 'operador', 'tag': 'vehiculo', 'mining': 'mina', 'type': 'tipo', 'tajo': 'tajo', 'weightTotal': 'ton', 'weightTotalTMS': 'tonh', 'material': 'material','createdAt': 'datetime'}, inplace=True)
# df_tripsTruck.rename(columns={'_id': 'travel_Id', 'date': 'fecha', 'turno': 'turno', 'name': 'operador', 'tag': 'vehiculo', 'mining': 'mina', 'type': 'tipo', 'tajo': 'tajo', 'weightTotal': 'ton', 'weightTotalTMS': 'tonh', 'material': 'material', 'status': 'statusMina', 'valid': 'validMina', 'createdAt': 'datetime'}, inplace=True)

##############################################################################

#####   TOTALES  #####

# timeTripMax = float(df['timeRetorno'].max())
# timeTripMin = float(df['timeRetorno'].min())
# timeTripMean = float(df['timeRetorno'].mean())

# weightMineral = int(df['mineral'].sum())
# weightDesmonte = int(df['desmonte'].sum())
# totalWeight = int(weightMineral + weightDesmonte)

#####################
# weightMineral = df['mineral'].sum()
# weightDesmonte = df['desmonte'].sum()
# totalWeight = weightMineral + weightDesmonte
#####################

# weightMineralTMS = float(weightMineral * 0.94)
# weightDesmonteTMS = float(weightDesmonte * 0.94)
# totalWeightTMS = float(totalWeight * 0.94)

# mineral = df[df['dominio'] == 'MINERAL']
# truck_min = mineral['tag'].drop_duplicates()
# truckMineral = len(truck_min)

# desmonte = df[df['dominio'] == 'DESMONTE']
# truck_des = desmonte['tag'].drop_duplicates()
# truckDesmonte = len(truck_des)

# totalTrucks = int(truckMineral + truckDesmonte)

##############################################################################


# SEPARAMOS POR RUTA

# b1 = 'YUM CARGUIO INTERIOR MINA - YUM CANCHA SUPERFICIE'
# b2 = 'YUM CARGUIO INTERIOR MINA - UCH CANCHA COLQUICOCHA'
# b3 = 'YUM CANCHA SUPERFICIE - UCH CANCHA COLQUICOCHA'
# b4 = 'UCH CANCHA COLQUICOCHA - UCH ECHADERO PLANTA'


# POR RUTA 

# df_b1 = df.query('ruta == "YUM CARGUIO INTERIOR MINA - YUM CANCHA SUPERFICIE"')
# df_b1 = df.query('ruta == @b1')
# df_b1.reset_index(drop=True, inplace=True)

# df_b2 = df.query('ruta == "YUM CARGUIO INTERIOR MINA - UCH CANCHA COLQUICOCHA"')
# df_b2 = df.query('ruta == @b2')
# df_b2.reset_index(drop=True, inplace=True)

# df_b3 = df.query('ruta == "YUM CANCHA SUPERFICIE - UCH CANCHA COLQUICOCHA"')
# df_b3 = df.query('ruta == @b3')
# df_b3.reset_index(drop=True, inplace=True)    

# df_b4 = df.query('ruta == "UCH CANCHA COLQUICOCHA - UCH ECHADERO PLANTA"')
# df_b4 = df.query('ruta == @b4')
# df_b4.reset_index(drop=True, inplace=True)



# TEST
# _df.groupby('ruta').agg({'weight_net': 'sum', 'tara': 'sum', 'weight_gross': 'sum'})

# _df['weight_net'].sum()
# _df['tara'].sum()
# _df['weight_gross'].sum()

# df.groupby('ruta').agg({'weight_net': 'sum', 'tara': 'sum', 'weight_gross': 'sum'})

# df.groupby('ruta')



##############################################################################

#####   BY TURN TRUCK  #####

# BYTURNO - PESO BRUTO
# df_turno = df.groupby('turno').agg({'weight_gross': 'sum'})
# df_turno.columns = ['weight_gross']
# df_turno = df_turno.T

# def byTurnTruck(df):
#     df_turno = df.groupby('turno').agg({'weight_gross': 'sum'})
#     df_turno.columns = ['weight_gross']
#     df_turno = df_turno.T
#     return df_turno

# BYTURNO - PESO NETO
# df_turno = df.groupby('turno').agg({'weight_net': 'sum'})
# df_turno.columns = ['weight_net']
# df_turno = df_turno.T

# BYTURNO - TARA
# df_turno = df.groupby('turno').agg({'tara': 'sum'})
# df_turno.columns = ['tara']
# df_turno = df_turno.T

# BYTURNO - WEIGHT
# df_turno = df.groupby('turno').agg({'weight': 'sum'})
# df_turno.columns = ['weight']
# df_turno = df_turno.T

# def byTurnTruck(df):
#     df_turno = df.groupby('turno').agg({'weight': 'sum'})
#     df_turno.columns = ['weight']
#     df_turno = df_turno.T
#     return df_turno

##############################################################################

# BY RUTA

# df_ruta = df_trips[['ruta', 'weight_net', 'tara', 'weight_gross']].groupby(['ruta']).agg({'weight_net': 'sum', 'tara': 'sum', 'weight_gross': 'sum'})
# df_ruta.columns = ['weight_net', 'tara', 'weight_gross']
# df_ruta.reset_index(inplace=True)

# df_rutaTurn = df_trips[['ruta', 'turno', 'weight_net', 'tara', 'weight_gross']].groupby(['ruta', 'turno']).agg({'weight_net': 'sum', 'tara': 'sum', 'weight_gross': 'sum'})
# df_rutaTurn = df_rutaTurn.reset_index()
# df_rutaTurn = df_rutaTurn.pivot(index='ruta', columns='turno', values=['weight_net', 'tara', 'weight_gross'])
# df_rutaTurn = df_rutaTurn.reset_index()

# df_ruta = df_ruta.merge(df_rutaTurn, on=['ruta'], how='left')
# df_ruta.fillna(0, inplace=True)


# BY TRUCKS

# df_truckProduction = df_trips.groupby(['tag']).agg({'weight_net': 'sum', 'tara': 'sum', 'weight_gross': 'sum', 'timeInoperation': ['sum', 'mean']})
# df_truckProduction.columns = ['weight_net', 'tara', 'weight_gross', 'timeInoperation', 'meanTime']
# df_truckProduction.reset_index(inplace=True)

# df_truckTurn = df_trips.groupby(['tag', 'turno']).agg({'weight_net': 'sum', 'tara': 'sum', 'weight_gross': 'sum'})
# df_truckTurn = df_truckTurn.reset_index()
# df_truckTurn = df_truckTurn.pivot(index='tag', columns='turno', values=['weight_net', 'tara', 'weight_gross'])
# df_truckTurn = df_truckTurn.reset_index()

# df_truckProduction = df_truckProduction.merge(df_truckTurn, on=['tag'], how='left')
# df_truckProduction.fillna(0, inplace=True)


# BY TRUCKS RUTA

# df_truckProduction = df_trips.groupby(['tag', 'ruta']).agg({'weight_net': 'sum', 'tara': 'sum', 'weight_gross': 'sum', 'timeInoperation': 'sum'})
# df_truckProduction.columns = ['weight_net', 'tara', 'weight_gross', 'timeInoperation']
# df_truckProduction.reset_index(inplace=True)

# df_truckTurn = df_trips.groupby(['tag', 'ruta', 'turno']).agg({'weight_net': 'sum', 'tara': 'sum', 'weight_gross': 'sum'})
# df_truckTurn = df_truckTurn.reset_index()
# df_truckTurn = df_truckTurn.pivot(index=['tag', 'ruta'], columns='turno', values=['weight_net', 'tara', 'weight_gross'])
# df_truckTurn = df_truckTurn.reset_index()

# df_truckProduction = df_truckProduction.merge(df_truckTurn, on=['tag', 'ruta'], how='left')
# df_truckProduction.fillna(0, inplace=True)

# PESO BRUTO - TOTAL

# weight_total = float(df['weight_gross'].sum())

# NUMERO DE VIAJES

# n_travels = int(df['start'].count())


#TO GRAPHIC
# def toGraphic(df, a, b):
#     df_graphic = df.groupby([a, b]).agg({'mining': 'count', 'weight_gross': 'sum'})
#     df_graphic.columns = ['totalTrips', 'weight_gross']
#     df_graphic.reset_index(inplace=True)
#     df_graphic = df_graphic.pivot(index=b, columns=a, values='weight_gross').T
#     df_graphic = df_graphic.reset_index()
#     df_graphic = df_graphic.fillna(0)


##############################################################################

#####   BEST SCORE   #####

# MEJOR SCORE

# c = 'date'
# c = 'week'
# c = 'month'

# WEIGHT_GROSS
# def bestScoreTruck(df, c):
#     _df = df.groupby([c]).agg({'weight_gross': 'sum', 'timeIda': 'mean', 'timeRetorno': 'mean', 'operationTruck_Id': 'count'})
#     idx = _df['weight_gross'].idxmax()
#     df_score = _df.loc[idx]
#     df_score['periodo'] = idx
#     return df_score

# WEIGHT
# def bestScore(df, c):
#     _df = df.groupby([c]).agg({'weight': 'sum', 'timeIda': 'mean', 'timeRetorno': 'mean', 'operationTruck_Id': 'count'})
#     idx = _df['weight'].idxmax()
#     df_score = _df.loc[idx]
#     df_score['periodo'] = idx
#     return df_score


##############################################################################

#####   TO GRAPHIC   #####

# a = 'date'
# a = 'hhmm'
# b = 'turno'

# WEIGHT_GROSS
# df_graphic = df.groupby(['hhmm', 'turno']).agg({'mining': 'count', 'weight_gross': 'sum'})
# df_graphic.columns = ['totalTrips', 'weight_gross']
# df_graphic.reset_index(inplace=True)
# df_graphic = df_graphic.pivot(index='turno', columns='hhmm', values='weight_gross').T
# df_graphic = df_graphic.reset_index()
# df_graphic = df_graphic.fillna(0)

# WEIGHT_GROSS
# def toGraphicTruck(df, a, b):
#     df_graphic = df.groupby([a, b]).agg({'mining': 'count', 'weight_gross': 'sum'})
#     df_graphic.columns = ['totalTrips', 'weight_gross']
#     df_graphic.reset_index(inplace=True)
#     df_graphic = df_graphic.pivot(index=b, columns=a, values='weight_gross').T
#     df_graphic = df_graphic.reset_index()
#     df_graphic = df_graphic.fillna(0)
#     return df_graphic

# WEIGHT
# df_graphic = df.groupby(['hhmm', 'turno']).agg({'mining': 'count', 'weight': 'sum'})
# df_graphic.columns = ['totalTrips', 'weight']
# df_graphic.reset_index(inplace=True)
# df_graphic = df_graphic.pivot(index='turno', columns='hhmm', values='weight').T
# df_graphic = df_graphic.reset_index()
# df_graphic = df_graphic.fillna(0)

# WEIGHT
# def toGraphicTruck(df, a, b):
#     df_graphic = df.groupby([a, b]).agg({'mining': 'count', 'weight': 'sum'})
#     df_graphic.columns = ['totalTrips', 'weight']
#     df_graphic.reset_index(inplace=True)
#     df_graphic = df_graphic.pivot(index=b, columns=a, values='weight').T
#     df_graphic = df_graphic.reset_index()
#     df_graphic = df_graphic.fillna(0)
#     return df_graphic

##############################################################################

# BY RUTA

# ruta

# type
# tajo
# dominio

# df_ruta = df.groupby(['ruta', 'dominio']).agg({'weight_gross': 'sum'})
# df_ruta



# BY TRUCK

# df_truckProduction = df.groupby(['tag']).agg({'weight_gross': 'sum'})

# # Tonelaje por camiones (dominio, disponibilidad)
# df_truckProduction = df.groupby(['tag', 'dominio']).agg({'weight_gross': 'sum'})
# df_truckProduction = df_truckProduction.reset_index()





# OBTENEMOS DATA - TRUCK

## PRIMERA FORMA ##

##########
# df_truckProduction = df.groupby(['tag', 'dominio']).agg({'weight': 'sum'}).unstack(fill_value=0)
# df_truckProduction.columns = df_truckProduction.columns.droplevel()
# df_truckProduction.reset_index(inplace=True)
# df_truckProduction.columns = ['tag', 'desmonte', 'mineral', 'otro']
# df_truckProduction['weightTotal'] = df_truckProduction['desmonte'] + df_truckProduction['mineral'] + df_truckProduction['otro']
# df_truckProduction['p_desmonte'] = (df_truckProduction['desmonte'] / df_truckProduction['weightTotal']) * 100
# df_truckProduction['p_mineral'] = (df_truckProduction['mineral'] / df_truckProduction['weightTotal']) * 100
# df_truckProduction['p_otro'] = (df_truckProduction['otro'] / df_truckProduction['weightTotal']) * 100
# df_truckProduction['p_weightTotal'] = df_truckProduction['p_desmonte'] + df_truckProduction['p_mineral'] + df_truckProduction['p_otro']
# df_truckProduction

##########

# df_truckTurn = df.groupby(['tag', 'turno']).agg({'weight': 'sum'}).unstack(fill_value=0)
# df_truckTurn.columns = df_truckTurn.columns.droplevel()
# df_truckTurn.reset_index(inplace=True)
# df_truckTurn.columns = ['tag', 'DIA', 'NOCHE']

# df_truckProduction = df_truckProduction.merge(df_truckTurn, on=['tag'], how='left')


## SEGUNDA FORMA ##

# df_truckProduction = df.groupby(['tag']).agg({'weightTotal': ['count', 'sum'], 'mineral': 'sum', 'desmonte': 'sum', 'timeInoperation': ['sum', 'mean']})
# df_truckProduction.columns = ['totalTrips', 'totalWeight', 'mineral', 'desmonte', 'timeInoperation', 'meanTime']
# df_truckProduction.reset_index(inplace=True)

# df_truckTurn = df.groupby(['tag', 'turno']).agg({'weightTotal': 'sum'})
# df_truckTurn = df_truckTurn.reset_index()
# df_truckTurn = df_truckTurn.pivot(index='tag', columns='turno', values='weightTotal')
# df_truckTurn = df_truckTurn.reset_index()

# df_truckProduction = df_truckProduction.merge(df_truckTurn, on=['tag'], how='left')
# df_truckProduction.fillna(0, inplace=True)

##############################################################################

#####   BY RUTA   #####

## PRIMERA FORMA ##

##########
# df_ruta = df.groupby(['ruta', 'dominio']).agg({'weight': 'sum'}).unstack(fill_value=0)
# df_ruta.columns = df_ruta.columns.droplevel()
# df_ruta.reset_index(inplace=True)
# df_ruta.columns = ['ruta', 'desmonte', 'mineral', 'otro']
# df_ruta['weightTotal'] = df_ruta['desmonte'] + df_ruta['mineral'] + df_ruta['otro']

##########
# df_rutaTurn = df.groupby(['ruta', 'turno']).agg({'weight': 'sum'}).unstack(fill_value=0)
# df_rutaTurn.columns = df_rutaTurn.columns.droplevel()
# df_rutaTurn.reset_index(inplace=True)
# df_rutaTurn.columns = ['ruta', 'DIA', 'NOCHE']

# df_ruta = df_ruta.merge(df_rutaTurn, on=['ruta'], how='left')


## SEGUNDA FORMA ##

# ORIGIN
# df_ruta = df[['ruta', 'weightTotal', 'mineral', 'desmonte']].groupby(['ruta']).agg({'weightTotal': ['count', 'sum'], 'mineral': 'sum', 'desmonte': 'sum'})
# df_ruta.columns = ['totalTrips', 'totalWeight', 'mineral', 'desmonte']
# df_ruta.reset_index(inplace=True)

# df_rutaTurn = df[['ruta', 'turno', 'weightTotal']].groupby(['ruta', 'turno']).agg({'weightTotal': 'sum'})
# df_rutaTurn = df_rutaTurn.reset_index()
# df_rutaTurn = df_rutaTurn.pivot(index='ruta', columns='turno', values='weightTotal')
# df_rutaTurn = df_rutaTurn.reset_index()

# df_ruta = df_ruta.merge(df_rutaTurn, on=['ruta'], how='left')
# df_ruta.fillna(0, inplace=True)


# OPCIONAL
##########
# df_ruta = df[['ruta', 'weight']].groupby(['ruta']).agg({'weight': ['count', 'sum']})
# df_ruta.columns = ['totalTrips', 'totalWeight']
# df_ruta.reset_index(inplace=True)

##########
# df_rutaTurn = df[['ruta', 'turno', 'weight']].groupby(['ruta', 'turno']).agg({'weight': 'sum'})
# df_rutaTurn = df_rutaTurn.reset_index()
# df_rutaTurn = df_rutaTurn.pivot(index='ruta', columns='turno', values='weight')
# df_rutaTurn = df_rutaTurn.reset_index()

# df_ruta = df_ruta.merge(df_rutaTurn, on=['ruta'], how='left')
# df_ruta.fillna(0, inplace=True)

## TERCERA FORMA ##

##########
# df_ruta = df.groupby(['ruta', 'dominio']).agg({'weight': 'sum'}).unstack(fill_value=0)
# df_ruta.columns = df_ruta.columns.droplevel()
# df_ruta.reset_index(inplace=True)
# df_ruta.columns = ['ruta', 'desmonte', 'mineral', 'otro']
# df_ruta['weightTotal'] = df_ruta['desmonte'] + df_ruta['mineral'] + df_ruta['otro']

##########
# df_rutaTurn = df[['ruta', 'turno', 'weight']].groupby(['ruta', 'turno']).agg({'weight': 'sum'})
# df_rutaTurn = df_rutaTurn.reset_index()
# df_rutaTurn = df_rutaTurn.pivot(index='ruta', columns='turno', values='weight')
# df_rutaTurn = df_rutaTurn.reset_index()

# df_ruta = df_ruta.merge(df_rutaTurn, on=['ruta'], how='left')
# df_ruta.fillna(0, inplace=True)

##############################################################################

# POR CAMION - RUTA

# df.groupby(['tag']).agg({'weight_gross': 'sum'})
# df.groupby(['ruta']).agg({'weight_gross': 'sum'})

# RUTA

# df.groupby(['ruta', 'tag']).agg({'weight_gross': 'sum'})
# df.groupby(['ruta', 'name']).agg({'weight_gross': 'sum'})
# df.groupby(['ruta', 'tag', 'dominio']).agg({'weight_gross': 'sum'})

# df.groupby(['tag', 'dominio']).agg({'weight_gross': 'sum'})



##############################################################################

#####   EVENTS   #####

# df_events = df_truckTravels[['stop', 'stand', 'delay', 'maintenance']]

# obtener todos los nombres de eventos que tengan time mayor a 0
# df_events['nameStop'] = df_events['stop'].apply(lambda x: [a['name'] for a in x if a['time'] > 0] if x else None)
# df_events['nameStand'] = df_events['stand'].apply(lambda x: [a['name'] for a in x if a['time'] > 0] if x else None)
# df_events['nameDelay'] = df_events['delay'].apply(lambda x: [a['name'] for a in x if a['time'] > 0] if x else None)
# df_events['nameMaintenance'] = df_events['maintenance'].apply(lambda x: [a['name'] for a in x if a['time'] > 0] if x else None)



## PRIMERA OPCION
# def get_names(row):
#     names = []
#     for stop_info in row:
#         for event in stop_info:
#             # Verificar que 'event' sea un diccionario
#             if isinstance(event, dict):
#                 # Verificar que 'time' y 'name' estén presentes en el diccionario
#                 if 'time' in event and 'name' in event:
#                     if event['time'] > 0:
#                         names.append(event['name'])
#     return names

# df['eventos'] = df.apply(get_names, axis=1)

## SEGUNDA OPCION
# df['events'] = df.apply(lambda row: [event['name'] for col, events in row.items() if isinstance(events, list) for event in events if isinstance(event, dict) and event['time'] > 0], axis=1)

# df['events'] = df.apply(lambda row: [{'name': event['name'], 'time': event['time']} for col, events in row.items() if isinstance(events, list) for event in events if isinstance(event, dict) and event['time'] > 0], axis=1)

# localizar los eventos que esten vacios
# df.loc[df['events'].apply(lambda x: len(x) == 0)]

# localizar los eventos que no esten vacios
# df.loc[df['events'].apply(lambda x: len(x) > 0)]

# contar los eventos que esten vacios
# df['events'].apply(lambda x: len(x) == 0).sum()

# contar los eventos que no esten vacios
# df['events'].apply(lambda x: len(x) > 0).sum()

# def getEventsTrucks():
    
#     df_resume, df_operations = getTripsTrucking()
    
#     df_copy = df_resume.copy()
#     df_filter = df_copy.sort_values(by='createdAt', ascending=False)
    
#     df_filter = df_filter[['name', 'tag', 'events', 'createdAt']]
    
#     df_events = df_filter.loc[df_filter['events'].apply(lambda x: len(x) > 0)]

# reiniciar el index
# df_events.reset_index(drop=True, inplace=True)
    
#     return df_events