import pandas as pd
from rapidfuzz import process, fuzz

df_pred = pd.read_csv('resultados_modelo/predicciones_mañana.csv')
df_real = pd.read_csv('data/market/2026-02-09.csv')

nombres_real = df_real['name'].tolist()
resultados = []

for nombre in df_pred['player_name']:
    match, score, _ = process.extractOne(nombre, nombres_real, scorer=fuzz.token_sort_ratio)
    resultados.append({'pred': nombre, 'real': match, 'score': score})

df_match = pd.DataFrame(resultados)
print(df_match[df_match['score'] < 80].head(20))
print('Total con score bajo:', (df_match['score'] < 80).sum())
