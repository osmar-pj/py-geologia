from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pytz

def byPromedios(df):
    df_promedios = df.groupby([df['mining'], df['year'],df['month'], df['nro_month'], df['week'],df['date_extraction']]).agg({'ton': 'sum', 'tonh': 'sum','ley_ag': 'mean', 'ley_fe': 'mean', 'ley_mn': 'mean', 'ley_pb': 'mean', 'ley_zn': 'mean'}).reset_index()
    return df_promedios