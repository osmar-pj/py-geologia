from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

import os
import pytz

load_dotenv()
client = MongoClient(os.getenv('MONGO_URI'))

db = client['wapsi']

def getTripsTrucking():

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

    df_resume, df1 = getOperationsTruck()
    df_trips = df_trips.merge(df_resume[['operationTruck_Id', 'driver_Id', 'truck_Id', 'mining', 'ruta', 'date', 'turno']], on=['operationTruck_Id', 'driver_Id', 'truck_Id', 'mining', 'ruta', 'date'], how='left')
    df_trips.dropna(subset=['turno'], inplace=True)
    df_trips = df_trips.merge(df_drivers[['name', 'driver_Id']], on=['driver_Id'], how='left')
    df_trips = df_trips.merge(df_trucks[['tag', 'truck_Id']], on=['truck_Id'], how='left')

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
    df['weight'] = df.apply(lambda row: round(row['weight_net'] / 1000, 1) if 'YUM CARGUIO INTERIOR MINA - YUM CANCHA SUPERFICIE' not in row['ruta'] else row['weight_net'], axis=1)
    df['mineral'] = np.where(df['dominio'] == 'MINERAL', df['weight'], 0)
    df['desmonte'] = np.where(df['dominio'] == 'DESMONTE', df['weight'], 0)
    df['weightTotal'] = df['mineral'] + df['desmonte']
    return df, df_truckTravels

def getOperationsTruck():
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