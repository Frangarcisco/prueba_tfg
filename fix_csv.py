import pandas as pd

df = pd.read_csv('data/raw/DATASET_MAESTRO_TFG_corregido.csv', sep=';', encoding='utf-8-sig')
print('Filas:', len(df))
print('Columnas:', df.columns.tolist()[:5])
df.to_csv('data/raw/DATASET_MAESTRO_TFG_corregido.csv', index=False, encoding='utf-8')
print('Guardado correctamente con comas')
