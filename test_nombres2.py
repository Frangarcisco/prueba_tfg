import pandas as pd

df_maestro = pd.read_csv('data/raw/DATASET_MAESTRO_TFG_corregido.csv', low_memory=False)
df_real = pd.read_csv('data/market/2026-02-09.csv')

nombres_maestro = set(df_maestro['player_name'].unique())
nombres_real = set(df_real['name'].unique())

no_coinciden = nombres_maestro - nombres_real
print(f'Jugadores en maestro pero no en real: {len(no_coinciden)}')
print(f'Jugadores en real pero no en maestro: {len(nombres_real - nombres_maestro)}')
print(f'Jugadores que coinciden: {len(nombres_maestro & nombres_real)}')
print(f'Total jugadores en maestro: {len(nombres_maestro)}')
print(f'Total jugadores en real (9 feb): {len(nombres_real)}')
