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

@app.route('/truck', methods=['GET'])
def getDataTruck():

    periodo = request.args.get('period')
    value = request.args.get('value')
    ruta = request.args.get('ruta')
    
    print(ruta)

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
    df_tripsTruck = df_tripsTruck[['_id', 'date', 'turno', 'name', 'tag', 'mining', 'type', 'tajo', 'weightTotal', 'weightTotalTMS', 'material', 'hhmm', 'status', 'valid', 'createdAt']]
    # df_tripsTruck.sort_values(by='createdAt', ascending=False, inplace=True)
    df_tripsTruck.rename(columns={'_id': 'travel_Id', 'date': 'fecha', 'turno': 'turno', 'name': 'operador', 'tag': 'vehiculo', 'mining': 'mina', 'type': 'tipo', 'tajo': 'tajo', 'weightTotal': 'ton', 'weightTotalTMS': 'tonh', 'material': 'material', 'hhmm': 'hora', 'status': 'statusMina', 'valid': 'validMina', 'createdAt': 'datetime'}, inplace=True)
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
    df_tripsWagon = df_tripsWagon[['_id', 'date', 'turno', 'name', 'tag', 'mining', 'totalValidWagons', 'weight', 'weightTotalTMS', 'material', 'hhmm', 'status', 'valid', 'createdAt']]
    df_tripsWagon.rename(columns={'_id': 'travel_Id', 'date': 'fecha', 'turno': 'turno', 'name': 'operador', 'tag': 'vehiculo', 'mining': 'mina', 'totalValidWagons': 'vagones','weight': 'ton', 'weightTotalTMS': 'tonh', 'material': 'material', 'hhmm': 'hora', 'status': 'statusMina', 'valid': 'validMina','createdAt': 'datetime'}, inplace=True)
    wagon = df_tripsWagon.to_dict('records')
    
    df_total = pd.concat([df_tripsTruck, df_tripsWagon])
    df_total['statusGeology'] = 'OreControl'
    df_total['validGeology'] = 1
    df_total = df_total.fillna({'vagones': 0})
    df_total = df_total.fillna('')

    df_total.sort_values(by='datetime', ascending=True, inplace=True)
    
    total = df_total.to_dict('records')
    
    return jsonify({'total': total})

@app.route('/datageology', methods=['GET'])
def dataGeology():
    
    # mes = request.args.get('month')
    # mina = request.args.get('mining')
    
    # periodo = request.args.get('period')
    # value = request.args.get('value')
    
    df_geology, df_columns = getGeology()
    
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
        'columns': columns
    }
    
    return jsonify(data)

@app.route('/analysis', methods=['POST'])
def geo_analysis():
    
    data = request.get_json()
    arr = data['arr']
    
    df_geology, df_main = getGeology()
    
    df_result = df_geology.groupby(arr).agg({'ton': 'sum', 'tonh': 'sum','ley_ag': 'mean', 'ley_fe': 'mean', 'ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'}).reset_index()
    result = df_result.to_dict('records')
    
    return jsonify(result)

@app.route('/analysis2', methods=['GET'])
def geo_analysis2():
    ts = request.args.get('ts')
    mining = request.args.get('mining')
    df_geology, df_main = getGeology()
    months = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO','JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE','DICIEMBRE']
    idxMonth = datetime.fromtimestamp(int(ts)).month - 1
    month = months[idxMonth]
    year = datetime.fromtimestamp(int(ts)).strftime('%Y')
    df_main['tonh*ley_ag'] = df_main['tonh'] * df_main['ley_ag']
    df_main['tonh*ley_fe'] = df_main['tonh'] * df_main['ley_fe']
    df_main['tonh*ley_mn'] = df_main['tonh'] * df_main['ley_mn']
    df_main['tonh*ley_pb'] = df_main['tonh'] * df_main['ley_pb']
    df_main['tonh*ley_zn'] = df_main['tonh'] * df_main['ley_zn']
    df1 = df_main.query('month == @month and year == @year and mining == @mining').groupby(['date_extraction']).agg({'tonh': 'sum', 'tonh*ley_ag': 'sum', 'tonh*ley_fe': 'sum', 'tonh*ley_mn': 'sum', 'tonh*ley_pb': 'sum', 'tonh*ley_zn': 'sum' }).reset_index()
    df1['Ag'] = df1['tonh*ley_ag'] / df1['tonh']
    df1['Fe'] = df1['tonh*ley_fe'] / df1['tonh']
    df1['Mn'] = df1['tonh*ley_mn'] / df1['tonh']
    df1['Pb'] = df1['tonh*ley_pb'] / df1['tonh']
    df1['Zn'] = df1['tonh*ley_zn'] / df1['tonh']
    df1.dropna(inplace=True)
    df1['date_extraction'] = pd.to_datetime(df1['date_extraction'], format='%d/%m/%Y')
    df1['timestamp'] = df1['date_extraction'].apply(lambda x: datetime.strptime(x.strftime('%d/%m/%Y'), '%d/%m/%Y').timestamp())
    df1.sort_values(by=['timestamp'], inplace=True)
    result = df1.to_dict('records')
    return jsonify(result)


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