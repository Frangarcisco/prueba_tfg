import pandas as pd

df_maestro = pd.read_csv('data/raw/DATASET_MAESTRO_TFG_corregido.csv')
df_real = pd.read_csv('data/market/2026-02-09.csv')

nombres_maestro = set(df_maestro['player_name'].unique())
nombres_real = set(df_real['name'].unique())

coinciden = nombres_maestro & nombres_real
no_coinciden = nombres_maestro - nombres_real

print(f'Coinciden: {len(coinciden)}')
print(f'No coinciden: {len(no_coinciden)}')
print('Ejemplos que no coinciden:')
print(sorted(no_coinciden)[:20])
