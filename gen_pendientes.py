import pandas as pd
from rapidfuzz import process, fuzz

df_maestro = pd.read_csv('data/raw/DATASET_MAESTRO_TFG_corregido.csv', low_memory=False)
df_real = pd.read_csv('data/market/2026-02-09.csv')

nombres_maestro = df_maestro['player_name'].unique()
nombres_real = df_real['name'].tolist()
nombres_real_set = set(nombres_real)

resultados = []
for nombre in nombres_maestro:
    if nombre in nombres_real_set:
        resultados.append({'nombre_maestro': nombre, 'sugerencia': nombre, 'score': 100})
    else:
        match, score, _ = process.extractOne(nombre, nombres_real, scorer=fuzz.token_sort_ratio)
        resultados.append({'nombre_maestro': nombre, 'sugerencia': match, 'score': score})

df_match = pd.DataFrame(resultados)
df_match = df_match[df_match['score'] < 100].sort_values('score')
df_match.to_csv('tabla_pendientes.csv', index=False)
print(f'Pendientes de revisar: {len(df_match)}')
