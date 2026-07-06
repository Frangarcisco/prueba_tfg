import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from modelo_tfg import cargar_datos, crear_features, entrenar_modelo

# ─────────────────────────────────────────────
# 🔧 CONFIGURACIÓN (TOCA SOLO ESTO)
# ─────────────────────────────────────────────
PLAYER_NAME = "Fermín"     # ← CAMBIA AQUÍ EL JUGADOR
FECHA_INICIO = "2025-10-01"  # ← CAMBIA AQUÍ
FECHA_FIN    = "2025-12-20"  # ← CAMBIA AQUÍ
HORIZONTE = 7                # 1, 3 o 7 días

# ─────────────────────────────────────────────
# CARGA Y MODELO
# ─────────────────────────────────────────────
df = cargar_datos("data/raw/DATASET_MAESTRO_TFG.csv")
df = crear_features(df)

modelo, res = entrenar_modelo(
    df,
    horizonte=HORIZONTE,
    fecha_inicio=FECHA_INICIO,
    fecha_fin=FECHA_FIN
)

df_test = res['df_test'].copy()
y_pred = res['y_pred']

# Añadir predicciones al dataframe
df_test['pred_xgb'] = y_pred

# ─────────────────────────────────────────────
# BASELINE NAIVE (lag-1)
# ─────────────────────────────────────────────
df_full = df.sort_values(['player_id', 'date'])
df_full['var_ayer'] = df_full.groupby('player_id')['marketValue'].diff(1)

df_test = df_test.merge(
    df_full[['player_id', 'date', 'var_ayer']],
    on=['player_id', 'date'],
    how='left'
)

print("\nDEBUG:")
print("Filas totales test:", len(df_test))

df_check = df_test[df_test['player_name'] == PLAYER_NAME]
print("Filas jugador:", len(df_check))

df_check = df_check[
    (df_check['date'] >= FECHA_INICIO) &
    (df_check['date'] <= FECHA_FIN)
]
print("Filas tras filtro fecha:", len(df_check))

# ─────────────────────────────────────────────
# FILTRAR JUGADOR Y FECHAS
# ─────────────────────────────────────────────
df_plot = df_test[
    (df_test['player_name'] == PLAYER_NAME) &
    (df_test['date'] >= FECHA_INICIO) &
    (df_test['date'] <= FECHA_FIN)
].copy()

target_col = f"target_{HORIZONTE}d"

# Ordenar por fecha (por si acaso)
df_plot = df_plot.sort_values('date')

# ─────────────────────────────────────────────
# 📊 GRÁFICA
# ─────────────────────────────────────────────
plt.figure(figsize=(12, 6))

plt.plot(df_plot['date'], df_plot[target_col],
         label="Real", linewidth=2)

plt.plot(df_plot['date'], df_plot['pred_xgb'],
         label="XGBoost", linestyle='--')

plt.plot(df_plot['date'], df_plot['var_ayer'],
         label="Naive (lag-1)", linestyle=':')

plt.title(f"Predicción vs Real — {PLAYER_NAME} ({HORIZONTE}d)", fontsize=14)
plt.xlabel("Fecha")
plt.ylabel("Variación (€)")
plt.legend()
plt.grid(alpha=0.2)

plt.tight_layout()
plt.show()

# ─────────────────────────────────────────────
# 📊 EXTRA: ERROR ACUMULADO
# ─────────────────────────────────────────────
df_plot['error_xgb'] = abs(df_plot[target_col] - df_plot['pred_xgb'])
df_plot['error_naive'] = abs(df_plot[target_col] - df_plot['var_ayer'])

plt.figure(figsize=(12, 4))

plt.plot(df_plot['date'], df_plot['error_xgb'],
         label="Error XGBoost")

plt.plot(df_plot['date'], df_plot['error_naive'],
         label="Error Naive")

plt.title(f"Error absoluto — {PLAYER_NAME}", fontsize=13)
plt.legend()
plt.grid(alpha=0.2)

plt.tight_layout()
plt.show()