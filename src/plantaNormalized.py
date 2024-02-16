import pandas as pd

df = pd.read_csv('../data/planta.csv')

df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.year
df['zona'] = df['zona'].replace('Yumpag', 'YUMPAG')
df['dominio'] = df['dominio'].replace('POLIMETALICO ', 'Polimetálico')
df['ubication'] = df['ubication'].replace('CANCHA 2', 'Cancha 2')
df['ubication'] = df['ubication'].replace('Cancha colquicocha', 'Cancha Colquicocha')
df['ubication'] = df['ubication'].replace('Cancha2', 'Cancha 2')
df['cod_tableta'] = df['cod_tableta'].astype(str)
df['tajo'] = df['tajo'].replace(['Tj 6432', 'Tj  6432'], 'TJ 6432')
df['tajo'] = df['tajo'].replace(['Tj 6432-1', 'TJ6432-1'], 'TJ 6432-1')
df['tajo'] = df['tajo'].replace(['Tj  6609'], 'TJ 6609')
df['tajo'] = df['tajo'].replace(['Tj  6608', 'Tj 6608'], 'TJ 6608')
df['tajo'] = df['tajo'].replace(['Tj  6618'], 'TJ 6618')
df['tajo'] = df['tajo'].replace(['Tj 6408'], 'TJ 6408')
df['tajo'] = df['tajo'].replace(['Tj 6618'], 'TJ 6618')
df['tajo'] = df['tajo'].replace(['Tj 6431-1', 'Tj  6431-1'], 'TJ 6431-1')
df['tajo'] = df['tajo'].replace(['Tj 6488', 'Tj  6488', 'Tj6488'], 'TJ 6488')
df['tajo'] = df['tajo'].replace(['Tj 500-11p', 'Tj 500-11P'], 'TJ 500-11P')
df['tajo'] = df['tajo'].replace(['Tj 500-5P', 'Tj 500-5p'], 'TJ 500-5P')
df['tajo'] = df['tajo'].replace(['Tj 500-7P'], 'TJ 500-7P')
df['tajo'] = df['tajo'].replace(['Tj.500-3p'], 'TJ 500-3P')
df['tajo'] = df['tajo'].replace(['Sn6431-2'], 'SN 6431-2')
df['tajo'] = df['tajo'].replace(['Tj 300-3p'], 'TJ 300-3P')
df['tajo'] = df['tajo'].replace(['Tj 400-1p', 'Tj 400-1P'], 'TJ 400-1P')
df['tajo'] = df['tajo'].replace(['Tj 500-8P-S1'], 'TJ 500-8P-S1')

# Dominio, tajo, zona, veta se guardara en el controlador NODE formato ARRAY

df.to_json('planta.json', orient='records')