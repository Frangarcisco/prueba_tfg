# run_experimentos.py
import sys
sys.path.insert(0, '.')

from modelo.modelo_tfg import cargar_datos, crear_features
from modelo.baselines import calcular_baselines
from modelo.experimentos import (
    generar_splits_expanding,
    generar_splits_rolling,
    ejecutar_experimento
)
import pandas as pd

HORIZONTE = 1   # empieza solo con 1 día para validar

# ── Carga (igual que en modelo_tfg.py) ──────────────
df = cargar_datos("data/raw/DATASET_MAESTRO_TFG.csv")
df = crear_features(df)

# ── Baselines sobre el split original (últimos 30 días) ──
from sklearn.model_selection import train_test_split
fecha_corte = df['date'].max() - pd.Timedelta(days=30)
df_train_base = df[df['date'] <= fecha_corte]
df_test_base  = df[df['date'] >  fecha_corte]

print("\n=== BASELINES ===")
baselines = calcular_baselines(df_train_base, df_test_base, HORIZONTE)

# ── Expanding window ─────────────────────────────────
print("\n=== EXPANDING WINDOW ===")
splits_exp = generar_splits_expanding(df, meses_minimos=2, paso_meses=1)
df_exp = ejecutar_experimento(df, splits_exp, HORIZONTE, "expanding", "resultados/expanding")

# ── Rolling window ───────────────────────────────────
print("\n=== ROLLING WINDOW ===")
splits_rol = generar_splits_rolling(df, ventana_dias=90, paso_dias=30)
df_rol = ejecutar_experimento(df, splits_rol, HORIZONTE, "rolling", "resultados/rolling")

# ── Comparativa final en pantalla ────────────────────
print("\n=== COMPARATIVA EXPANDING vs ROLLING ===")
print(f"Expanding — MAE medio: {df_exp['mae'].mean():,.0f}€  std: {df_exp['mae'].std():,.0f}€")
print(f"Rolling   — MAE medio: {df_rol['mae'].mean():,.0f}€  std: {df_rol['mae'].std():,.0f}€")