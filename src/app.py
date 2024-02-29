from flask import Flask, jsonify, request
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd
import numpy as np
import json
# WAGONS
from model.wagonB2.listWagons import getTripsWagon
from model.wagonB2.event import getEvents
from model.wagonB2.groupBy import byPique, byLocomotive, byTurn, bestScore, toGraphic, completeWeek, completeMonth

#
from model.truckB1.trips import getTripsTruck

# TRUCKS
from model.truckB3.listTrucks import getTripsTrucking
from model.truckB3.groupBy import byRuta, byTruck, byTurnTruck, bestScoreTruck, toGraphicTruck, completeWeekTruck, completeMonthTruck, getEventsTrucks

# GEOLOGY

from model.geology.list import getGeology
from model.geology.groupBy import byPromedios

import os

from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

client = MongoClient(os.getenv('MONGO_URI'))

db = client['wapsi']

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

@app.route('/truck', methods=['GET'])
def getDataTruck():
    periodo = request.args.get('period')
    value = request.args.get('value')
    ruta = request.args.get('ruta')

    if value is not None:
        try:
            value = int(value)
            print('convertido a entero')
        except ValueError:
            print('No se puede convertir a entero')
    else:
        print('No hay valor')

    df_trips, df_truckTravels = getTripsTrucking()
    
    if ruta == 'B1':
        b1 = 'YUM CARGUIO INTERIOR MINA - YUM CANCHA SUPERFICIE'
        df_trips = df_trips.query('ruta == @b1')
    elif ruta == 'B2':
        b2 = 'YUM CARGUIO INTERIOR MINA - UCH CANCHA COLQUICOCHA'
        df_trips = df_trips.query('ruta == @b2')
    elif ruta == 'B3':
        b3 = 'YUM CANCHA SUPERFICIE - UCH CANCHA COLQUICOCHA'
        df_trips = df_trips.query('ruta == @b3')
    elif ruta == 'B4':
        b4 = 'UCH CANCHA COLQUICOCHA - UCH ECHADERO PLANTA'
        df_trips = df_trips.query('ruta == @b4')
        
    if df_trips.empty:
        data = {
            'total': {
                'timeTripMax': 0,
                'timeTripMin': 0,
                'timeTripMean': 0,
                
                'weightMineral': 0,
                'weightDesmonte': 0,
                
                'weightMineralTMS': 0,
                'weightDesmonteTMS': 0,
                
                'weightTotal': 0,
                'totalTrips': 0,
                
                'truckMineral': 0,
                'truckDesmonte': 0,
                
                'totalTrucks': 0,
                'totalWeightTMS': 0,
            },
            'trips': [],
            'events': [],
            'turn': {},
            'graphic': [],
            'truckProduction': [],
            'ruta': [],
            'score': {}
        }
        return jsonify({'data': data})

    if periodo == 'D':
        value = value + 5 * 60 * 60 * 1000 # El timestamp enviado es GMT-5
        date = datetime.fromtimestamp(value / 1000).strftime("%d/%m/%Y")
        df = df_trips.query('date == @date')
        a = 'hhmm'
        b = 'turno'
        c = 'date'
    elif periodo == 'S':
        df = df_trips.query('week == @value')
        a = 'date'
        b = 'turno'
        c = 'week'
    elif periodo == 'M':
        month = value
        df = df_trips.query('nro_month == @month')
        a = 'date'
        b = 'turno'
        c = 'month'

    df_events = getEventsTrucks(df)

    df_score = bestScoreTruck(df_trips, c)

    if df.empty:
        events = df_events.to_dict('records')
        score = df_score.to_dict()
        trips = df_trips.to_dict('records')
        data = {
            'total': {
                'timeTripMax': 0,
                'timeTripMin': 0,
                'timeTripMean': 0,
                
                'weightMineral': 0,
                'weightDesmonte': 0,
                
                'weightMineralTMS': 0,
                'weightDesmonteTMS': 0,
                
                'weightTotal': 0,
                'totalTrips': 0,
                
                'truckMineral': 0,
                'truckDesmonte': 0,
                
                'totalTrucks': 0,
                'totalWeightTMS': 0,
                
            },
            'trips': trips,
            'events': events,
            'turn': {},
            'graphic': [],
            'truckProduction': [],
            'ruta': [],
            'score': score
        }
        return jsonify({'data': data})
    
    df_truck = byTruck(df)
    
    df_graphic = toGraphicTruck(df, a, b)

    df_ruta = byRuta(df)

    df_turn = byTurnTruck(df)

    if periodo == 'D':
        df_graphic['date'] = df_graphic['hhmm']
    elif periodo == 'S':
        df_graphic = completeWeekTruck(df_graphic)
    elif periodo == 'M':
        df_graphic = completeMonthTruck(df_graphic)

    timeTripMax = float(df['timeRetorno'].max())
    timeTripMin = float(df['timeRetorno'].min())
    timeTripMean = float(df['timeRetorno'].mean())
    
    weightMineral = df['mineral'].sum()
    weightDesmonte = df['desmonte'].sum()
    totalWeight = weightMineral + weightDesmonte
    
    weightMineralTMS = float(weightMineral * 0.94)
    weightDesmonteTMS = float(weightDesmonte * 0.94)
    totalWeightTMS = float(totalWeight * 0.94)
    
    mineral = df[df['dominio'] == 'MINERAL']
    truck_min = mineral['tag'].drop_duplicates()
    truckMineral = len(truck_min)
    
    desmonte = df[df['dominio'] == 'DESMONTE']
    truck_des = desmonte['tag'].drop_duplicates()
    truckDesmonte = len(truck_des)
    
    totalTrucks = int(truckMineral + truckDesmonte)

    totalTrips = int(df['start'].count())

    df_final_trips = df.sort_values(by='createdAt', ascending=False)
    df_final_trips = df_final_trips[['tag', 'name', 'ruta', 'weightTotal', 'createdAt']]

    trips = df_final_trips.to_dict('records')
    events = df_events.to_dict('records')
    turn = df_turn.to_dict('index')
    graphic = df_graphic.to_dict('records')
    truck = df_truck.to_dict('records')
    ruta = df_ruta.to_dict('records')
    score = df_score.to_dict()

    data = {
        'total': {
            'timeTripMax': timeTripMax,
            'timeTripMin': timeTripMin,
            'timeTripMean': timeTripMean,
            
            'weightMineral': weightMineral,
            'weightDesmonte': weightDesmonte,
            
            'weightMineralTMS': weightMineralTMS,
            'weightDesmonteTMS': weightDesmonteTMS,
            
            'truckMineral': truckMineral,
            'truckDesmonte': truckDesmonte,
            
            'totalTrucks': totalTrucks,
            'totalWeightTMS': totalWeightTMS,
            
            'weightTotal': totalWeight,
            'totalTrips': totalTrips
        },
        'trips': trips,
        'events': events,
        'turn': turn,
        'graphic': graphic,
        'truckProduction': truck,
        'ruta': ruta,
        'score': score
    }

    return jsonify({'data': data})


@app.route('/wagon', methods=['GET'])
def getDataWagon():
    # get data from HTTP URL
    periodo = request.args.get('period') # S, M, D
    value = request.args.get('value') # timestamp
    if value is not None:
        try:
            value = int(value)  # Convertir a entero
            print('convertido a entero')
        except ValueError:
            print('No se puede convertir a entero')
    else:
        print('No hay valor')
    df_trips, df_travels = getTripsWagon()
    if periodo == 'D':
        # value in timestamp change to date "%d/%m/%Y"
        value = value + 5 * 60 * 60 * 1000 # El timestamp enviado es GMT-5
        date = datetime.fromtimestamp(value / 1000).strftime("%d/%m/%Y")
        df = df_trips.query('date == @date')
        a = 'hhmm'
        b = 'turno'
        c = 'date'
    elif periodo == 'S':
        df = df_trips.query('week == @value')
        a = 'date'
        b = 'turno'
        c = 'week'
    elif periodo == 'M':
        month = value
        df = df_trips.query('nro_month == @month')
        a = 'date'
        b = 'turno'
        c = 'month'
    
    df_events = getEvents(df)
    df_score = bestScore(df_trips, c)
    if df.empty:
        events = df_events.to_dict('records')
        score = df_score.to_dict()
        trips = df_trips.to_dict('records')
        data = {
            'total': {
                'timeTripMax': 0,
                'timeTripMin': 0,
                'timeTripMean': 0,
                
                'weightMineral': 0,
                'weightDesmonte': 0,
                
                'weightMineralTMS': 0,
                'weightDesmonteTMS': 0,
                
                'wagonMineral': 0,
                'wagonDesmonte': 0,
                
                'totalWagons': 0,
                'totalWeight': 0,
                'totalWeightTMS': 0,
                
                'totalTrips': 0,
                'mineralTrips': 0,
                'desmonteTrips': 0
            },
            'trips': trips,
            'events': events,
            'turn': {},
            'graphic': [],
            'locomProduction': [],
            'pique': [],
            'score': score
        }
        return jsonify({'data': data})

    df_locomotive = byLocomotive(df)
    df_graphic = toGraphic(df, a, b)
    df_pique = byPique(df)
    df_turn = byTurn(df)
    if periodo == 'D':
        df_graphic['date'] = df_graphic['hhmm']
    elif periodo == 'S':
        df_graphic = completeWeek(df_graphic)
    elif periodo == 'M':
        df_graphic = completeMonth(df_graphic)
        
    timeTripMax = float(df['timeRetorno'].max())
    timeTripMin = float(df['timeRetorno'].min())
    timeTripMean = float(df['timeRetorno'].mean())

    weightMineral = int(df['mineral'].sum() * 8)
    weightDesmonte = int(df['desmonte'].sum() * 8)
    totalWeight = int(weightMineral + weightDesmonte)

    weightMineralTMS = float(weightMineral * 0.94)
    weightDesmonteTMS = float(weightDesmonte * 0.94)
    totalWeightTMS = float(totalWeight * 0.94)

    wagonMineral = int(df['polimetalico'].sum() + df['alabandita'].sum() + df['carbonato'].sum())
    wagonDesmonte = int(df['desmonte'].sum())
    totalWagons = int(df['totalValidWagons'].count())
    
    totalTrips = int(df['start'].count())
    mineralTrips = int(df.query('desmonte != 0')['start'].count())
    desmonteTrips = int(df.query('desmonte == 0')['start'].count())

    df_trips = df.sort_values(by='createdAt', ascending=False)

    trips = df_trips.to_dict('records')
    events = df_events.to_dict('records')
    turn = df_turn.to_dict('index')
    graphic = df_graphic.to_dict('records')
    locomotive = df_locomotive.to_dict('records')
    pique = df_pique.to_dict('records')
    score = df_score.to_dict()

    data = {
        'total': {
            'timeTripMax': timeTripMax,
            'timeTripMin': timeTripMin,
            'timeTripMean': timeTripMean,
            
            'weightMineral': weightMineral,
            'weightDesmonte': weightDesmonte,
            
            'weightMineralTMS': weightMineralTMS,
            'weightDesmonteTMS': weightDesmonteTMS,
            
            'wagonMineral': wagonMineral,
            'wagonDesmonte': wagonDesmonte,
            
            'totalWagons': totalWagons,
            'totalWeight': totalWeight,
            'totalWeightTMS': totalWeightTMS,
            
            'totalTrips': totalTrips,
            'mineralTrips': mineralTrips,
            'desmonteTrips': desmonteTrips
        },
        'trips': trips,
        'events': events,
        'turn': turn,
        'graphic': graphic,
        'locomProduction': locomotive,
        'pique': pique,
        'score': score
    }
    return jsonify({'data': data})

@app.route('/tripWagon', methods=['GET'])
def tripWagon():
    df_trips, df_travels = getTripsWagon()
    df_trips.sort_values(by='createdAt', ascending=False, inplace=True)
    trips = df_trips.head(40).to_dict('records')
    return jsonify({'data': trips})

# Camiones con TAG
# @app.route('/tripTruck', methods=['GET'])
# def tripTruck():
#     # df_trips = getTripsTruck()
#     df_trips = pd.read_csv('./data/tripTruck.csv')
#     df_trips.sort_values(by='min', ascending=False, inplace=True)
#     trips = df_trips.head(40).to_dict('records')
#     return jsonify({'data': trips})


@app.route('/tripTruck', methods=['GET'])
def tripTruck():
    df_trips, df_truckTravels  = getTripsTrucking()
    df_trips.sort_values(by='createdAt', ascending=False, inplace=True)
    trips = df_trips.head(40).to_dict('records')
    return jsonify(trips)

@app.route('/tripGeology', methods=['GET'])
def tripGeology():
    # CAMIONES
    df_tripsTruck, df_truckTravels  = getTripsTrucking()
    df_tripsTruck = df_tripsTruck.query('ruta == "YUM CANCHA SUPERFICIE - UCH CANCHA COLQUICOCHA" or ruta == "YUM CARGUIO INTERIOR MINA - UCH CANCHA COLQUICOCHA" ')
    # crear otra columna valid, donde si el status es completo que sea 1 y si es incompleto que sea 1
    df_tripsTruck['valid'] = np.where(df_tripsTruck['status'] == 'completo', 1, 0)
    
    df_tripsTruck['weightTotalTMS'] = (df_tripsTruck['weightTotal'] * 0.94).round(1)
    df_tripsTruck['material'] = np.select([(df_tripsTruck['mineral'] > 0) & (df_tripsTruck['desmonte'] == 0), (df_tripsTruck['desmonte'] > 0) & (df_tripsTruck['mineral'] == 0), (df_tripsTruck['mineral'] > 0) & (df_tripsTruck['desmonte'] > 0)], ['MINERAL', 'DESMONTE', 'MIXTO'], default='OTRO')
    df_tripsTruck = df_tripsTruck[['_id', 'date_extraction', 'turn', 'name', 'tag', 'mining', 'type', 'tajo', 'weightTotal', 'weightTotalTMS', 'dominio', 'hhmm', 'status', 'valid', 'createdAt']]
    # df_tripsTruck.sort_values(by='createdAt', ascending=False, inplace=True)
    # df_tripsTruck.rename(columns={'_id': 'travel_Id', 'date': 'fecha', 'turno': 'turno', 'name': 'operador', 'tag': 'vehiculo', 'mining': 'mina', 'type': 'tipo', 'tajo': 'tajo', 'weightTotal': 'ton', 'weightTotalTMS': 'tonh', 'material': 'material', 'hhmm': 'hora', 'status': 'statusMina', 'valid': 'validMina', 'createdAt': 'datetime'}, inplace=True)
    # fecha_hoy = datetime.now().strftime('%d/%m/%Y')
    # df_tripsTruck = df_tripsTruck.query('fecha == @fecha_hoy')
    truck = df_tripsTruck.to_dict('records')
    # trips = df_tripsTruck.head(10).to_dict('records')
    
    # VAGONES
    df_tripsWagon, df_travels = getTripsWagon()
    df_tripsWagon['status'] = 'completo'
    df_tripsWagon['valid'] = np.where(df_tripsWagon['status'] == 'completo', 1, 0)
    df_tripsWagon['weightTotalTMS'] = (df_tripsWagon['weight'] * 0.94).round(1)
    df_tripsWagon['material'] = np.select([(df_tripsWagon['mineral'] > 0) & (df_tripsWagon['desmonte'] == 0), (df_tripsWagon['desmonte'] > 0) & (df_tripsWagon['mineral'] == 0), (df_tripsWagon['mineral'] > 0) & (df_tripsWagon['desmonte'] > 0)], ['MINERAL', 'DESMONTE', 'MIXTO'], default='OTRO')
    df_tripsWagon = df_tripsWagon[['_id', 'date_extraction', 'turn', 'name', 'tag', 'mining', 'totalValidWagons', 'weight', 'weightTotalTMS', 'dominio', 'hhmm', 'status', 'valid', 'createdAt']]
    # df_tripsWagon.rename(columns={'_id': 'travel_Id', 'date': 'fecha', 'turno': 'turno', 'name': 'operador', 'tag': 'vehiculo', 'mining': 'mina', 'totalValidWagons': 'vagones','weight': 'ton', 'weightTotalTMS': 'tonh', 'material': 'material', 'hhmm': 'hora', 'status': 'statusMina', 'valid': 'validMina','createdAt': 'datetime'}, inplace=True)
    wagon = df_tripsWagon.to_dict('records')
    
    df_total = pd.concat([df_tripsTruck, df_tripsWagon])
    df_total['statusGeology'] = 'OreControl'
    df_total['validGeology'] = 1
    df_total = df_total.fillna({'vagones': 0})
    df_total = df_total.fillna('')

    df_total.sort_values(by='datetime', ascending=True, inplace=True)
    # control de envio de datos
    total = df_total.head(10).to_dict('records')
    
    return jsonify({'total': total})

@app.route('/datageology', methods=['GET'])
def dataGeology():
    df_geology, df_columns, df_prog = getGeology()
    
    year = df_geology['year'].unique().tolist()
    month = df_geology['month'].unique().tolist()
    nro_month = df_geology['nro_month'].unique().tolist()
    status = df_geology['status'].unique().tolist()
    mining = df_geology['mining'].unique().tolist()
    ubication = df_geology['ubication'].unique().tolist()
    turn = df_geology['turn'].unique().tolist()
    level = df_geology['level'].unique().tolist()
    type = df_geology['type'].unique().tolist()
    veta = df_geology['veta'].unique().tolist()
    tajo = df_geology['tajo'].unique().tolist()
    dominio = df_geology['dominio'].unique().tolist()
    
    columns = pd.DataFrame(df_columns.dtypes, columns=['type'])
    columns.reset_index(inplace=True)
    columns.rename(columns={'index': 'name'}, inplace=True)
    columns['type'] = columns['type'].astype(str)
    columns = columns.to_dict('records')
    
    data = {
        'year': year,
        'month': month,
        'nro_month': nro_month,
        'status': status,
        'mining': mining,
        'ubication': ubication,
        'turn': turn,
        'level': level,
        'type': type,
        'veta': veta,
        'tajo': tajo,
        'dominio': dominio,
        'columns': columns # Aqui se envia la data de las columnas para el filtered
    }
    
    return jsonify(data)

@app.route('/analysis', methods=['POST'])
def geo_analysis():
    # Para la tabla dinamica
    data = request.get_json()
    ts = data['ts']
    category = data['category']
    arr = data['arr']
    trips = data['trips']
    df = pd.DataFrame(trips)
    fixed = ["year", "month", "mining"]
    # timestamp = int(ts)
    months = ["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"]
    month = months[datetime.fromtimestamp(ts).month - 1]
    year = datetime.fromtimestamp(ts).year
    if category == "planta":
        df['dominio'] = df['dominio'].apply(lambda x: moda_data(x))
        df['tajo'] = df['tajo'].apply(lambda x: moda_data(x))
        df['zona'] = df['zona'].apply(lambda x: moda_data(x))
    #     df['veta'] = df['veta'].apply(lambda x: moda_data(x))
    def leyPonderada(x):
        return np.average(x, weights=df.loc[x.index, 'tonh'])

    both = [
        {"mining": "YUMPAG"},
        {"mining": "UCHUCCHACUA"}
    ]
    # grouped is concat fixed and arr
    grouped = fixed + arr
    values = ['tonh', 'ley_ag', 'ley_fe', 'ley_mn', 'ley_pb', 'ley_zn']
    df_body = df.groupby(grouped).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

    data = []
    for i in range(len(both)):
        df_bodyFiltered = df_body[( df_body['year'] == year) & (df_body['month'] == month) & (df_body['mining'] == both[i]['mining'])]
        df_footer = df_bodyFiltered.groupby("year").agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
        # concat body and footer
        df_footer['year'] = "TOTAL"
        _df = pd.concat([df_bodyFiltered, df_footer])
        _df.replace(np.nan, None, inplace=True)
        body = _df.to_dict('records')
        data.append({"body": body})

    return jsonify({
        "data": data
    })


@app.route('/analysis/planta', methods=['POST'])
def geo_analysis_planta():
    # Para la tabla dinamica
    data = request.get_json()
    ts = data['ts']
    category = data['category']
    arr = data['arr']
    
    months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']
    idxMonth = datetime.fromtimestamp(int(ts)).month - 1
    month = months[idxMonth]
    year = datetime.fromtimestamp(int(ts)).year

    trips = db['plantas']
    df_trips = pd.DataFrame(list(trips.find()))
    df_trips['_id'] = df_trips['_id'].astype(str)
    fixed = ['year','month']
    if category == "planta":
            df_trips['dominio'] = df_trips['dominio'].apply(lambda x: moda_data(x))
            df_trips['tajo'] = df_trips['tajo'].apply(lambda x: moda_data(x))
            df_trips['zona'] = df_trips['zona'].apply(lambda x: moda_data(x))
            df_trips['veta'] = df_trips['veta'].apply(lambda x: moda_data(x))
    def leyPonderada(x):
            return np.average(x, weights=df_trips.loc[x.index, 'tonh'])

    # grouped is concat fixed and arr
    grouped = fixed + arr
    values = ['tonh', 'ley_ag', 'ley_fe', 'ley_mn', 'ley_pb', 'ley_zn']
    df_body = df_trips.groupby(grouped).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

    df_bodyFiltered = df_body[( df_body['year'] == year) & (df_body['month'] == month)]
    df_footer = df_bodyFiltered.groupby("year").agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
    # concat body and footer
    df_footer['year'] = "TOTAL"
    _df = pd.concat([df_bodyFiltered, df_footer])
    _df.replace(np.nan, None, inplace=True)
    body = _df.to_dict('records')

    return jsonify({
        "data": body
    })

@app.route('/analysisIn', methods=['GET'])
def geo_analysisIn():
    ts = request.args.get('ts')
    mining = request.args.get('mining')

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
    now = datetime.now()
    nowTimestamp = now.timestamp()
    idxNonthNow = datetime.fromtimestamp(int(ts)).month - 1
    monthNow = months[idxNonthNow]
    yearNow = now.year

    total_ton_prog = df3['ton_prog'].sum()
    total_ton_ejec_cumm = df3.query('timestamp < @nowTimestamp')['tonh'].sum()
    total_ton_prog_cumm = df3.query('timestamp < @nowTimestamp')['ton_prog'].sum()

    df4 = df3.fillna(0)
    df5 = df4.query('timestamp < @nowTimestamp').copy()

    total_ley_ag_prog = np.average(df4['ley_ag_prog'], weights=df4['ton_prog'])
    total_ley_ag_prog_cumm = np.average(df5['ley_ag_prog'], weights=df5['ton_prog'])
    total_ley_ag_ejec_cumm = np.average(df5['ley_ag'], weights=df5['tonh'])

    total_ley_fe_prog = np.average(df4['ley_fe_prog'], weights=df4['ton_prog'])
    total_ley_fe_prog_cumm = np.average(df5['ley_fe_prog'], weights=df5['ton_prog'])
    total_ley_fe_ejec_cumm = np.average(df5['ley_fe'], weights=df5['tonh'])

    total_ley_mn_prog = np.average(df4['ley_mn_prog'], weights=df4['ton_prog'])
    total_ley_mn_prog_cumm = np.average(df5['ley_mn_prog'], weights=df5['ton_prog'])
    total_ley_mn_ejec_cumm = np.average(df5['ley_mn'], weights=df5['tonh'])

    total_ley_pb_prog = np.average(df4['ley_pb_prog'], weights=df4['ton_prog'])
    total_ley_pb_prog_cumm = np.average(df5['ley_pb_prog'], weights=df5['ton_prog'])
    total_ley_pb_ejec_cumm = np.average(df5['ley_pb'], weights=df5['tonh'])

    total_ley_zn_prog = np.average(df4['ley_zn_prog'], weights=df4['ton_prog'])
    total_ley_zn_prog_cumm = np.average(df5['ley_zn_prog'], weights=df5['ton_prog'])
    total_ley_zn_ejec_cumm = np.average(df5['ley_zn'], weights=df5['tonh'])

    meta = {
        'ton': {
            'total_ton_prog': total_ton_prog,
            'total_ton_ejec_cumm': total_ton_ejec_cumm,
            'total_ton_prog_cumm': total_ton_prog_cumm,
            'percent_ejec': total_ton_ejec_cumm * 100 / total_ton_prog,
            'percent_prog': total_ton_prog_cumm * 100 / total_ton_prog
        },
        'ley_ag': {
            'total_ley_prog': total_ley_ag_prog,
            'total_ley_ejec_cumm': total_ley_ag_ejec_cumm,
            'total_ley_prog_cumm': total_ley_ag_prog_cumm,
            'percent_ejec': total_ley_ag_ejec_cumm * 100 / total_ley_ag_prog,
            'percent_prog': total_ley_ag_prog_cumm * 100 / total_ley_ag_prog
        },
        'ley_fe': {
            'total_ley_prog': total_ley_fe_prog,
            'total_ley_ejec_cumm': total_ley_fe_ejec_cumm,
            'total_ley_prog_cumm': total_ley_fe_prog_cumm,
            'percent_ejec': total_ley_fe_ejec_cumm * 100 / total_ley_fe_prog,
            'percent_prog': total_ley_fe_prog_cumm * 100 / total_ley_fe_prog
        },
        'ley_mn': {
            'total_ley_prog': total_ley_mn_prog,
            'total_ley_ejec_cumm': total_ley_mn_ejec_cumm,
            'total_ley_prog_cumm': total_ley_mn_prog_cumm,
            'percent_ejec': total_ley_mn_ejec_cumm * 100 / total_ley_mn_prog,
            'percent_prog': total_ley_mn_prog_cumm * 100 / total_ley_mn_prog
        },
        'ley_pb': {
            'total_ley_prog': total_ley_pb_prog,
            'total_ley_ejec_cumm': total_ley_pb_ejec_cumm,
            'total_ley_prog_cumm': total_ley_pb_prog_cumm,
            'percent_ejec': total_ley_pb_ejec_cumm * 100 / total_ley_pb_prog,
            'percent_prog': total_ley_pb_prog_cumm * 100 / total_ley_pb_prog
        },
        'ley_zn': {
            'total_ley_prog': total_ley_zn_prog,
            'total_ley_ejec_cumm': total_ley_zn_ejec_cumm,
            'total_ley_prog_cumm': total_ley_zn_prog_cumm,
            'percent_ejec': total_ley_zn_ejec_cumm * 100 / total_ley_zn_prog,
            'percent_prog': total_ley_zn_prog_cumm * 100 / total_ley_zn_prog
        },
    }
    
    result = df3.to_dict('records')
    return jsonify({
        'meta': meta,
        'result': result
    })

@app.route('/analysisOut', methods=['GET'])
def geo_analysisOut():
    ts = request.args.get('ts')
    mining = request.args.get('mining')

    months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO','JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE','DICIEMBRE']
    idxMonth = datetime.fromtimestamp(int(ts)).month - 1
    month = months[idxMonth]
    year = datetime.fromtimestamp(int(ts)).year

    trips = db['plantas']
    df_trips = pd.DataFrame(list(trips.find()))
    df_trips['_id'] = df_trips['_id'].astype(str)

    df_prog = pd.read_csv('./data/prog_planta.csv')
    df_finos = pd.read_csv('./data/finos_planta.csv')
    df_lab = pd.read_csv('./data/lab_planta.csv')

    df_prog.fillna(np.nan, inplace=True)
    df_finos.fillna(np.nan, inplace=True)
    df_lab.fillna(np.nan, inplace=True)

    df_prog.replace(np.nan, None, inplace=True)
    df_finos.replace(np.nan, None, inplace=True)
    df_lab.replace(np.nan, None, inplace=True)

    df_prog['date'] = pd.to_datetime(df_prog['date'])
    df_finos['date'] = pd.to_datetime(df_finos['date'])
    df_lab['date'] = pd.to_datetime(df_lab['date'])

    def leyPonderada(x):
            return np.average(x, weights=df_trips.loc[x.index, 'tonh'])

    df_planta = df_trips.groupby('date').agg({'ton': 'sum', 'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_fe': leyPonderada, 'ley_mn': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()

    # # concat all dataframes
    df_planta = df_planta.merge(df_prog, on='date', how='left')
    df_planta = df_planta.merge(df_finos, on='date', how='left')
    df_planta = df_planta.merge(df_lab, on='date', how='left')
    df_planta['month'] = df_planta['date'].dt.month
    df_planta['month'] = df_planta['month'].apply(lambda x: months[x-1])
    df_planta['year'] = df_planta['date'].dt.year
    df_planta['timestamp'] = df_planta['date'].apply(lambda x: x.timestamp())  

    _df = df_planta[(df_planta['month'] == month) & (df_planta['year'] == year)]
    result = _df.to_dict('records')
    return jsonify({
        'result': result,
        "meta": []
    })

@app.route('/nsr', methods=['GET'])
def nsr():
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

    def nsr(df):
            df['ag_rec'] = [0.28877 * x if x < 2.8 else 0.0422 * np.log(x) + 0.768505 for x in df['ley_ag']]
            df['pb_rec'] = [2.2829 * x if x < 0.4 else 0.0024 * x + 0.896 for x in df['ley_pb']]
            df['zn_rec'] = [0.81564 * x if x < 0.55 else 0.14627 * np.log(x) + 0.60619 if x < 7.85 else 0.808 for x in df['ley_zn']]
            df['nsr'] = df['ag_rec'] * pointValues['vp_ag'] * df['ley_ag'] + df['pb_rec'] * pointValues['vp_pb'] * df['ley_pb'] + df['zn_rec'] * pointValues['vp_zn'] * df['ley_zn']
            df['ag_eq'] = df['nsr'] / (pointValues['vp_ag'] * df['ag_rec'])
            return df

    pointValues = {
            'vp_ag': 13,
            'vp_pb': 14.69,
            'vp_zn': 13.76,
    }
    pilas = db['pilas']
    df_pilas = pd.DataFrame(list(pilas.find()))
    df_pilas['_id'] = df_pilas['_id'].astype(str)
    df_resumen = df_pilas.query('status == "Cancha" and (statusPila == "waitBeginDespacho" or statusPila == "Despachando")').reset_index(drop=True)
    df_resumen['dominio'] = df_resumen['dominio'].apply(lambda x: moda_data(x))

    def leyPonderada(x):
            return np.average(x, weights=df_resumen.loc[x.index, 'tonh'])

    df1 = df_resumen.groupby(['ubication', 'dominio']).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
    df2 = df_resumen.groupby(['ubication']).agg({'tonh': 'sum', 'ley_ag': leyPonderada, 'ley_pb': leyPonderada, 'ley_zn': leyPonderada}).reset_index()
    df1 = nsr(df1)
    df2 = nsr(df2)
    df1['index'] = df1.index
    df2['index'] = df2.index
    return jsonify({
        "data": df1.to_dict('records'),
        "total": df2.to_dict('records')
    })

@app.route('/list_geology', methods=['GET'])
def list_geology():
    listtrips = db['listtrips']
    df_trips = pd.DataFrame(list(listtrips.find()))
    df_trips['_id'] = df_trips['_id']
    df_trips.sort_values(by=['timestamp'], ascending=False, inplace=True)
    trips = df_trips.to_dict('records')
    return jsonify(trips)
    

@app.route('/ruma', methods=['GET'])
def rumas():
    rumas = db['rumas']
    df_ruma = pd.DataFrame(list(rumas.find()))
    ruma = df_ruma.to_json(orient='records')
    return jsonify(ruma)

@app.route('/update', methods=['GET'])
def update():
    df_trips = getTripsTruck()
    return jsonify({'data': 'updated'})

@app.errorhandler(404)
def not_found(error=None):
    response = jsonify({
        'message': 'Resource Not Found: ' + request.url,
        'status': 404
    })
    response.status_code = 404
    return response

if __name__ == '__main__':
    # app.run(debug=True)
    app.run(debug=True, host='0.0.0.0', port=8081)