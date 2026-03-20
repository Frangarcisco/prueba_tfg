import pandas as pd

# Cargar mercado del 28 enero (AnaliticaFantasy)
df_analitica = pd.read_csv('data/market/2026-01-28.csv')

# Cargar dataset maestro filtrado al 28 enero
df_maestro = pd.read_csv('data/raw/DATASET_MAESTRO_TFG.csv')
df_maestro['date'] = pd.to_datetime(df_maestro['date'])
df_28 = df_maestro[df_maestro['date'].dt.date == pd.Timestamp('2026-01-28').date()]

print('Analitica:', len(df_analitica))
print('Maestro 28 ene:', len(df_28))

# Cruzar por market_value exacto
df_merge = df_analitica.merge(
    df_28[['marketValue', 'player_name']],
    left_on='market_value',
    right_on='marketValue',
    how='inner'
)
print('Cruzados:', len(df_merge))
print(df_merge[['id', 'name', 'player_name', 'market_value']].head(10))
