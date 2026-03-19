"""
=============================================================
  TFG - Predicción Valor de Mercado LaLiga Fantasy
  Modelo: XGBoost con predicción a 1, 3 y 7 días
=============================================================
Requisitos:
    pip install xgboost scikit-learn pandas numpy matplotlib seaborn

Uso:
    python modelo_tfg.py
=============================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
import os
import warnings
warnings.filterwarnings('ignore')

from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
DATASET_PATH = "DATASET_MAESTRO_TFG.csv"
OUTPUT_DIR   = "resultados_modelo"
HORIZONTES   = [1, 3, 7]   # días a predecir

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# 1. CARGA Y LIMPIEZA
# ─────────────────────────────────────────────
def cargar_datos(path):
    print("📂 Cargando dataset...")
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None)
    df = df.sort_values(['player_id', 'date']).reset_index(drop=True)

    # Eliminar columna bids (toda a 0, no aporta)
    df.drop(columns=['bids'], inplace=True, errors='ignore')

    # Rellenar nulos de equipo/nombre con forward fill por jugador
    df['equipo']  = df.groupby('player_id')['equipo'].ffill().bfill()
    df['nombre']  = df.groupby('player_id')['nombre'].ffill().bfill()
    df['posicion'] = df.groupby('player_id')['posicion'].ffill().bfill()

    # Rellenar resto de nulos numéricos con 0
    cols_num = df.select_dtypes(include=[np.number]).columns
    df[cols_num] = df[cols_num].fillna(0)

    print(f"✅ Dataset cargado: {df.shape[0]} filas, {df.shape[1]} columnas")
    print(f"   Jugadores: {df['player_id'].nunique()}")
    print(f"   Rango fechas: {df['date'].min().date()} → {df['date'].max().date()}")
    return df


# ─────────────────────────────────────────────
# 2. FEATURE ENGINEERING
# ─────────────────────────────────────────────
def crear_features(df):
    print("\n⚙️  Creando features...")

    df = df.sort_values(['player_id', 'date']).copy()

    # --- TARGET: variación de marketValue a N días ---
    for h in HORIZONTES:
        df[f'target_{h}d'] = (
            df.groupby('player_id')['marketValue'].shift(-h) - df['marketValue']
        )
        # También en porcentaje (útil para análisis)
        df[f'target_pct_{h}d'] = (
            df[f'target_{h}d'] / df['marketValue'] * 100
        ).round(4)

    # --- LAGS de marketValue ---
    for lag in [1, 2, 3, 7, 14]:
        df[f'mv_lag_{lag}d'] = df.groupby('player_id')['marketValue'].shift(lag)

    # --- VARIACIONES PASADAS ---
    for lag in [1, 3, 7]:
        df[f'mv_var_past_{lag}d'] = df['marketValue'] - df.groupby('player_id')['marketValue'].shift(lag)
        df[f'mv_pct_past_{lag}d'] = (df[f'mv_var_past_{lag}d'] / df.groupby('player_id')['marketValue'].shift(lag) * 100).round(4)

    # --- MEDIAS MÓVILES de puntos ---
    for ventana in [3, 7, 14]:
        df[f'puntos_ma_{ventana}d'] = (
            df.groupby('player_id')['puntos_totales']
            .transform(lambda x: x.shift(1).rolling(ventana, min_periods=1).mean())
        )
        df[f'mv_ma_{ventana}d'] = (
            df.groupby('player_id')['marketValue']
            .transform(lambda x: x.shift(1).rolling(ventana, min_periods=1).mean())
        )

    # --- FEATURES TEMPORALES ---
    df['dia_semana']  = df['date'].dt.dayofweek        # 0=Lunes, 6=Domingo
    df['dia_mes']     = df['date'].dt.day
    df['mes']         = df['date'].dt.month
    df['semana_año']  = df['date'].dt.isocalendar().week.astype(int)

    # --- CODIFICACIÓN DE POSICIÓN Y EQUIPO ---
    le_equipo = LabelEncoder()
    df['equipo_enc'] = le_equipo.fit_transform(df['equipo'].astype(str))
    df['posicion']   = df['posicion'].astype(int)

    # --- RATIO marketValue vs media de posición ---
    media_mv_pos = df.groupby(['date', 'posicion'])['marketValue'].transform('mean')
    df['ratio_mv_vs_posicion'] = df['marketValue'] / (media_mv_pos + 1)

    print(f"✅ Features creadas. Shape: {df.shape}")
    return df


# ─────────────────────────────────────────────
# 3. DEFINIR FEATURES DE ENTRADA
# ─────────────────────────────────────────────
FEATURES = [
    # Valor actual y lags
    'marketValue',
    'mv_lag_1d', 'mv_lag_2d', 'mv_lag_3d', 'mv_lag_7d', 'mv_lag_14d',
    # Variaciones pasadas
    'mv_var_past_1d', 'mv_var_past_3d', 'mv_var_past_7d',
    'mv_pct_past_1d', 'mv_pct_past_3d', 'mv_pct_past_7d',
    # Medias móviles
    'mv_ma_3d', 'mv_ma_7d', 'mv_ma_14d',
    # Stats de rendimiento
    'puntos_totales', 'marca_points', 'mins_played',
    'goals', 'goal_assist', 'saves', 'effective_clearance',
    'yellow_card', 'red_card', 'own_goals', 'goals_conceded',
    'total_scoring_att', 'won_contest', 'ball_recovery', 'poss_lost_all',
    # Acumulados
    'puntos_acumulados', 'partidos_jugados', 'media_puntos_hasta_fecha',
    'goles_acumulados', 'ranking_posicion_jornada',
    # Medias móviles de puntos
    'puntos_ma_3d', 'puntos_ma_7d', 'puntos_ma_14d',
    # Calendario
    'dias_prox_partido', 'is_paron_selecciones', 'dias_desde_inicio',
    'jornada',
    # Temporales
    'dia_semana', 'dia_mes', 'mes', 'semana_año',
    # Contexto
    'posicion', 'equipo_enc', 'ratio_mv_vs_posicion',
]


# ─────────────────────────────────────────────
# 4. ENTRENAMIENTO Y EVALUACIÓN
# ─────────────────────────────────────────────
def entrenar_modelo(df, horizonte):
    print(f"\n{'='*50}")
    print(f"🚀 Entrenando modelo para horizonte: {horizonte} día(s)")
    print(f"{'='*50}")

    target = f'target_{horizonte}d'

    # Filtrar filas con target válido y features completas
    features_disponibles = [f for f in FEATURES if f in df.columns]
    df_modelo = df[features_disponibles + [target, 'date', 'player_id', 'player_name']].dropna()

    print(f"   Filas para entrenamiento: {len(df_modelo)}")

    X = df_modelo[features_disponibles]
    y = df_modelo[target]
    fechas = df_modelo['date']

    # --- SPLIT TEMPORAL: últimos 30 días como test ---
    fecha_corte = fechas.max() - pd.Timedelta(days=30)
    train_mask = fechas <= fecha_corte
    test_mask  = fechas > fecha_corte

    X_train, X_test = X[train_mask], X[test_mask]
    y_train, y_test = y[train_mask], y[test_mask]

    print(f"   Train: {len(X_train)} filas hasta {fecha_corte.date()}")
    print(f"   Test:  {len(X_test)} filas")

    # --- MODELO XGBoost ---
    modelo = xgb.XGBRegressor(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        early_stopping_rounds=30,
        eval_metric='mae',
    )

    modelo.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # --- MÉTRICAS ---
    y_pred = modelo.predict(X_test)

    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2   = r2_score(y_test, y_pred)

    # MAE en porcentaje (más interpretable)
    mv_test = df_modelo.loc[test_mask, 'marketValue']
    mae_pct = (np.abs(y_test.values - y_pred) / mv_test.values * 100).mean()

    print(f"\n📊 RESULTADOS ({horizonte}d):")
    print(f"   MAE:      {mae:,.0f} € ({mae_pct:.2f}% del valor)")
    print(f"   RMSE:     {rmse:,.0f} €")
    print(f"   R²:       {r2:.4f}")

    # --- IMPORTANCIA DE FEATURES ---
    importancia = pd.DataFrame({
        'feature': features_disponibles,
        'importancia': modelo.feature_importances_
    }).sort_values('importancia', ascending=False)

    print(f"\n🔍 Top 10 features más importantes:")
    print(importancia.head(10).to_string(index=False))

    # --- GUARDAR RESULTADOS ---
    guardar_graficos(y_test, y_pred, importancia, horizonte, mae, mae_pct, r2)

    return modelo, {
        'horizonte': horizonte,
        'mae': mae,
        'mae_pct': mae_pct,
        'rmse': rmse,
        'r2': r2,
        'n_train': len(X_train),
        'n_test': len(X_test),
        'importancia': importancia,
        'modelo': modelo,
        'features': features_disponibles,
        'df_test': df_modelo[test_mask].copy(),
        'y_pred': y_pred,
    }


# ─────────────────────────────────────────────
# 5. GRÁFICOS
# ─────────────────────────────────────────────
def guardar_graficos(y_test, y_pred, importancia, horizonte, mae, mae_pct, r2):
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(f'Modelo XGBoost — Horizonte {horizonte} día(s)\nMAE: {mae:,.0f}€ ({mae_pct:.2f}%)  |  R²: {r2:.4f}',
                 fontsize=13, fontweight='bold')

    # 1. Real vs Predicho
    ax = axes[0]
    lim = max(abs(y_test.max()), abs(y_test.min())) * 1.1
    ax.scatter(y_test, y_pred, alpha=0.3, s=5, color='steelblue')
    ax.plot([-lim, lim], [-lim, lim], 'r--', lw=1.5, label='Predicción perfecta')
    ax.set_xlabel('Variación Real (€)')
    ax.set_ylabel('Variación Predicha (€)')
    ax.set_title('Real vs Predicho')
    ax.legend()
    ax.set_xlim(-lim, lim)
    ax.set_ylim(-lim, lim)

    # 2. Distribución de errores
    ax = axes[1]
    errores = y_pred - y_test.values
    ax.hist(errores, bins=60, color='steelblue', edgecolor='white', alpha=0.8)
    ax.axvline(0, color='red', lw=1.5, linestyle='--')
    ax.set_xlabel('Error (€)')
    ax.set_ylabel('Frecuencia')
    ax.set_title('Distribución del Error')

    # 3. Top 15 features
    ax = axes[2]
    top15 = importancia.head(15)
    ax.barh(top15['feature'][::-1], top15['importancia'][::-1], color='steelblue')
    ax.set_xlabel('Importancia')
    ax.set_title('Top 15 Features')

    plt.tight_layout()
    ruta = os.path.join(OUTPUT_DIR, f'resultados_{horizonte}d.png')
    plt.savefig(ruta, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"   💾 Gráfico guardado: {ruta}")


# ─────────────────────────────────────────────
# 6. PREDICCIÓN DE JUGADORES ESPECÍFICOS
# ─────────────────────────────────────────────
def predecir_jugadores(df, resultados, top_n=20):
    """Genera una tabla con predicciones para los últimos datos disponibles."""
    print(f"\n{'='*50}")
    print("🔮 PREDICCIONES PARA MAÑANA (último día disponible)")
    print(f"{'='*50}")

    ultimo_dia = df['date'].max()
    df_hoy = df[df['date'] == ultimo_dia].copy()

    print(f"   Fecha base: {ultimo_dia.date()}")
    print(f"   Jugadores con datos: {len(df_hoy)}")

    resultado_1d = resultados[1]
    modelo = resultado_1d['modelo']
    features = resultado_1d['features']

    features_ok = [f for f in features if f in df_hoy.columns]
    df_pred = df_hoy[features_ok].fillna(0)

    predicciones = modelo.predict(df_pred)
    df_hoy = df_hoy.copy()
    df_hoy['pred_var_1d'] = predicciones
    df_hoy['pred_mv_mañana'] = df_hoy['marketValue'] + predicciones
    df_hoy['pred_pct_1d'] = (predicciones / df_hoy['marketValue'] * 100).round(2)

    # Top subidas y bajadas
    tabla = df_hoy[['player_name', 'posicion', 'equipo', 'marketValue',
                     'pred_var_1d', 'pred_pct_1d', 'pred_mv_mañana']].copy()
    tabla = tabla.sort_values('pred_var_1d', ascending=False)

    print(f"\n📈 TOP {top_n} SUBIDAS PREDICHAS:")
    print(tabla.head(top_n).to_string(index=False))

    print(f"\n📉 TOP {top_n} BAJADAS PREDICHAS:")
    print(tabla.tail(top_n).to_string(index=False))

    # Guardar CSV
    ruta_csv = os.path.join(OUTPUT_DIR, 'predicciones_mañana.csv')
    tabla.to_csv(ruta_csv, index=False)
    print(f"\n💾 Predicciones guardadas: {ruta_csv}")

    return tabla


# ─────────────────────────────────────────────
# 7. RESUMEN FINAL
# ─────────────────────────────────────────────
def resumen_final(resultados):
    print(f"\n{'='*50}")
    print("📋 RESUMEN COMPARATIVO DE MODELOS")
    print(f"{'='*50}")

    filas = []
    for h, r in resultados.items():
        filas.append({
            'Horizonte': f'{h} día(s)',
            'MAE (€)': f"{r['mae']:,.0f}",
            'MAE (%)': f"{r['mae_pct']:.2f}%",
            'RMSE (€)': f"{r['rmse']:,.0f}",
            'R²': f"{r['r2']:.4f}",
            'Train': r['n_train'],
            'Test': r['n_test'],
        })

    resumen = pd.DataFrame(filas)
    print(resumen.to_string(index=False))

    ruta = os.path.join(OUTPUT_DIR, 'resumen_modelos.csv')
    resumen.to_csv(ruta, index=False)
    print(f"\n💾 Resumen guardado: {ruta}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  TFG — Predicción Valor Mercado LaLiga Fantasy")
    print("=" * 50)

    # 1. Cargar
    df = cargar_datos(DATASET_PATH)

    # 2. Features
    df = crear_features(df)

    # 3. Entrenar un modelo por horizonte
    resultados = {}
    for h in HORIZONTES:
        _, res = entrenar_modelo(df, h)
        resultados[h] = res

    # 4. Predicciones del último día
    predecir_jugadores(df, resultados)

    # 5. Resumen
    resumen_final(resultados)

    print(f"\n✅ ¡Todo listo! Resultados en la carpeta: {OUTPUT_DIR}/")