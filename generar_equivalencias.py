import pandas as pd
from rapidfuzz import process, fuzz

df_pred = pd.read_csv('resultados_modelo/predicciones_mañana.csv')
df_real = pd.read_csv('data/market/2026-02-09.csv')

nombres_real = df_real['name'].tolist()
resultados = []

for nombre in df_pred['player_name']:
    match, score, _ = process.extractOne(nombre, nombres_real, scorer=fuzz.token_sort_ratio)
    resultados.append({'nombre_maestro': nombre, 'nombre_analitica': match, 'score': score})

df_match = pd.DataFrame(resultados)
df_match.to_csv('tabla_equivalencias.csv', index=False)
print('Guardado tabla_equivalencias.csv')
print(f'Total jugadores: {len(df_match)}')
print(f'Coincidencias buenas (>=80): {(df_match["score"] >= 80).sum()}')
print(f'Requieren revision manual (<80): {(df_match["score"] < 80).sum()}')
