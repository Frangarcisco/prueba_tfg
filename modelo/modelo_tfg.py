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
DATASET_PATH = "data/raw/DATASET_MAESTRO_TFG_corregido.csv"
OUTPUT_DIR   = "resultados_modelo"
HORIZONTES   = [1, 3, 7]   # días a predecir

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# 1. CARGA Y LIMPIEZA
# ─────────────────────────────────────────────
def cargar_datos(path):
    print(" Cargando dataset...")
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.normalize()
    df = df.dropna(subset=['date'])
    df = df.sort_values(['player_id', 'date']).reset_index(drop=True)

    # Eliminar columna bids (toda a 0, no aporta)
    df.drop(columns=['bids'], inplace=True, errors='ignore')

    # Rellenar nulos de equipo/nombre con forward fill por jugador
    # Esto es para rellnar valores faltantes usando información del mismo jugador en días cercanos
    df['equipo']  = df.groupby('player_id')['equipo'].ffill().bfill()
    df['nombre']  = df.groupby('player_id')['nombre'].ffill().bfill()
    df['posicion'] = df.groupby('player_id')['posicion'].ffill().bfill()

    # Rellenar resto de nulos numéricos con 0
    cols_num = df.select_dtypes(include=[np.number]).columns
    df[cols_num] = df[cols_num].fillna(0)

    print(f" Dataset cargado: {df.shape[0]} filas, {df.shape[1]} columnas")
    print(f"   Jugadores: {df['player_id'].nunique()}")
    print(f"   Rango fechas: {df['date'].min().date()} → {df['date'].max().date()}")
    return df


# ─────────────────────────────────────────────
# 2. FEATURE ENGINEERING
# ─────────────────────────────────────────────

# Creo nuevas variables a partir de los datos originales para que el modelo pueda detectar patrones y hacer mejores predicciones.
def crear_features(df):
    print("\n  Creando features...")

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
    # Para suavizar el ruido
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
    # Comparo el valor del jugador con la media de su posición para contextualizarlo
    media_mv_pos = df.groupby(['date', 'posicion'])['marketValue'].transform('mean')
    df['ratio_mv_vs_posicion'] = df['marketValue'] / (media_mv_pos + 1)

    print(f" Features creadas. Shape: {df.shape}")
    return df


# ─────────────────────────────────────────────
# 3. DEFINIR FEATURES DE ENTRADA
# ─────────────────────────────────────────────

# la lista de columnas que el modelo va a usar como entrada

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
# ETIQUETAS LEGIBLES PARA LAS GRÁFICAS
# ─────────────────────────────────────────────
ETIQUETAS_FEATURES = {
    'marketValue': 'Valor de mercado actual',
    'mv_lag_1d': 'Precio hace 1 día',
    'mv_lag_2d': 'Precio hace 2 días',
    'mv_lag_3d': 'Precio hace 3 días',
    'mv_lag_7d': 'Precio hace 7 días',
    'mv_lag_14d': 'Precio hace 14 días',
    'mv_var_past_1d': 'Variación día -1',
    'mv_var_past_3d': 'Variación 3 días atrás',
    'mv_var_past_7d': 'Variación 7 días atrás',
    'mv_pct_past_1d': 'Variación % día -1',
    'mv_pct_past_3d': 'Variación % 3 días atrás',
    'mv_pct_past_7d': 'Variación % 7 días atrás',
    'mv_ma_3d': 'Media móvil precio 3 días',
    'mv_ma_7d': 'Media móvil precio 7 días',
    'mv_ma_14d': 'Media móvil precio 14 días',
    'puntos_totales': 'Puntos acumulados temporada',
    'marca_points': 'Puntuación DAZN (jornada)',
    'mins_played': 'Minutos jugados',
    'goals': 'Goles (jornada)',
    'goal_assist': 'Asistencias',
    'saves': 'Paradas',
    'effective_clearance': 'Despejes efectivos',
    'yellow_card': 'Tarjetas amarillas',
    'red_card': 'Tarjetas rojas',
    'own_goals': 'Goles en propia meta',
    'goals_conceded': 'Goles encajados',
    'total_scoring_att': 'Intentos de gol',
    'won_contest': 'Duelos ganados',
    'ball_recovery': 'Recuperaciones de balón',
    'poss_lost_all': 'Pérdidas de posesión',
    'puntos_acumulados': 'Puntos acumulados',
    'partidos_jugados': 'Partidos jugados',
    'media_puntos_hasta_fecha': 'Media de puntos (temporada)',
    'goles_acumulados': 'Goles acumulados',
    'ranking_posicion_jornada': 'Ranking en su posición',
    'puntos_ma_3d': 'Media móvil puntos 3 días',
    'puntos_ma_7d': 'Media móvil puntos 7 días',
    'puntos_ma_14d': 'Media móvil puntos 14 días',
    'dias_prox_partido': 'Días al próximo partido',
    'is_paron_selecciones': 'Parón de selecciones',
    'dias_desde_inicio': 'Días desde inicio de temporada',
    'jornada': 'Número de jornada',
    'dia_semana': 'Día de la semana',
    'dia_mes': 'Día del mes',
    'mes': 'Mes del año',
    'semana_año': 'Semana del año',
    'posicion': 'Posición del jugador',
    'equipo_enc': 'Equipo',
    'ratio_mv_vs_posicion': 'Ratio valor vs. posición',
}


# ─────────────────────────────────────────────
# 4. ENTRENAMIENTO Y EVALUACIÓN
# ─────────────────────────────────────────────
def calcular_splits_temporales(fechas, horizonte, dias_test=30, val_fraction=0.10):
    """
    Calcula las fechas de corte para train / validación / test dejando un
    margen (gap) de `horizonte` días en cada frontera. El motivo es que el
    target de un ejemplo (shift(-horizonte)) lee el marketValue `horizonte`
    días por delante de la fecha de la fila; sin este margen, una fila
    cercana al límite de un bloque leería información que pertenece al
    bloque siguiente.

    - Test: últimos `dias_test` días del dataset (idéntico al split
      original, para no romper la comparabilidad con las tablas ya
      descritas en la memoria).
    - Validación: usada solo para early stopping, nunca para reportar
      métricas. Su tamaño es una proporción (`val_fraction`) del
      histórico disponible antes del gap de test.
    - Train: el resto, con el mismo gap antes de validación.
    """
    fecha_max = fechas.max()
    fecha_min = fechas.min()

    fecha_corte_test = fecha_max - pd.Timedelta(days=dias_test)
    test_mask = fechas > fecha_corte_test

    fecha_fin_val = fecha_corte_test - pd.Timedelta(days=horizonte)
    dias_historicos = (fecha_fin_val - fecha_min).days
    dias_val = max(1, round(dias_historicos * val_fraction))
    fecha_inicio_val = fecha_fin_val - pd.Timedelta(days=dias_val - 1)

    fecha_fin_train = fecha_inicio_val - pd.Timedelta(days=horizonte + 1)
    train_mask = fechas <= fecha_fin_train
    val_mask   = (fechas >= fecha_inicio_val) & (fechas <= fecha_fin_val)

    if fecha_fin_train < fecha_min:
        raise ValueError(
            f"Histórico insuficiente para horizonte={horizonte}d con "
            f"val_fraction={val_fraction}: el train quedaría vacío o "
            f"empezaría antes del inicio del dataset."
        )

    return {
        'train_mask': train_mask, 'val_mask': val_mask, 'test_mask': test_mask,
        'fechas_corte': {
            'fin_train':   fecha_fin_train,
            'inicio_val':  fecha_inicio_val,
            'fin_val':     fecha_fin_val,
            'inicio_test': fecha_corte_test + pd.Timedelta(days=1),
        }
    }

def limpiar_X(df_x):
    """
    Convierte todas las columnas de X a float64, incluyendo columnas que
    tras la carga desde CSV hayan quedado con dtype 'object' o 'bool'
    (p. ej. is_paron_selecciones, media_puntos_hasta_fecha). Necesario
    porque XGBoost solo admite int, float, bool o category como dtype.
    """
    resultado = df_x.copy()
    for col in resultado.columns:
        if resultado[col].dtype == object or resultado[col].dtype == bool:
            resultado[col] = pd.to_numeric(resultado[col], errors='coerce').fillna(0).astype('float64')
        else:
            resultado[col] = resultado[col].astype('float64')
    return resultado

def entrenar_modelo(
    df,
    horizonte,
    train_mask=None,
    test_mask=None,
    fecha_inicio=None,
    fecha_fin=None
):
    print(f"\n{'='*50}")
    print(f" Entrenando modelo para horizonte: {horizonte} día(s)")
    print(f"{'='*50}")

    target = f'target_{horizonte}d'

    # Filtrar filas con target válido y features completas
    features_disponibles = [f for f in FEATURES if f in df.columns]
    df_modelo = df[features_disponibles + [target, 'date', 'player_id', 'player_name']].dropna()

    print(f"   Filas para entrenamiento: {len(df_modelo)}")

    X = df_modelo[features_disponibles]
    y = df_modelo[target]
    fechas = df_modelo['date']

    # =========================
    # PRIORIDAD DE SPLITS
    # =========================

    val_mask = None

    # 1. Split manual (máxima prioridad)
    if train_mask is not None and test_mask is not None:
        train_mask = train_mask.reindex(df_modelo.index, fill_value=False)
        test_mask  = test_mask.reindex(df_modelo.index, fill_value=False)

        print(f"   Train: {train_mask.sum()} filas")
        print(f"   Test:  {test_mask.sum()} filas")

    # 2. Split por fechas personalizadas (LO QUE QUIERES)
    elif fecha_inicio is not None and fecha_fin is not None:
        fecha_inicio = pd.to_datetime(fecha_inicio)
        fecha_fin    = pd.to_datetime(fecha_fin)

        test_mask  = (fechas >= fecha_inicio) & (fechas <= fecha_fin)
        train_mask = fechas < fecha_inicio

        print(f"   Train: {train_mask.sum()} filas hasta {fecha_inicio.date()}")
        print(f"   Test:  {test_mask.sum()} filas ({fecha_inicio.date()} → {fecha_fin.date()})")

    # 3. Split automático (fallback)
    else:
        s = calcular_splits_temporales(fechas, horizonte, dias_test=30, val_fraction=0.10)
        train_mask, val_mask, test_mask = s['train_mask'], s['val_mask'], s['test_mask']
        fc = s['fechas_corte']
        print(f"   Train: {train_mask.sum()} filas hasta {fc['fin_train'].date()}")
        print(f"   Val:   {val_mask.sum()} filas ({fc['inicio_val'].date()} → {fc['fin_val'].date()})")
        print(f"   Test:  {test_mask.sum()} filas desde {fc['inicio_test'].date()}")

    # =========================
    # SPLIT FINAL
    # =========================

    X_train = X[train_mask]
    X_test  = X[test_mask]
    y_train = y[train_mask]
    y_test  = y[test_mask]

    X_train = limpiar_X(X_train)
    X_test  = limpiar_X(X_test)

    if val_mask is not None:
        X_val = limpiar_X(X[val_mask])
        y_val = y[val_mask]
    else:
        X_val = X_test
        y_val = y_test

    # ⚠️ CHECK CLAVE (te evita bugs silenciosos)
    if len(X_test) == 0:
        raise ValueError("El test está vacío. Revisa el rango de fechas.")

    # =========================
    # MODELO
    # =========================

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
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    # =========================
    # MÉTRICAS
    # =========================

    y_pred = modelo.predict(X_test)

    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2   = r2_score(y_test, y_pred)

    mv_test = df_modelo.loc[test_mask, 'marketValue']
    mae_pct = (np.abs(y_test.values - y_pred) / mv_test.values * 100).mean()

    print(f"\n RESULTADOS ({horizonte}d):")
    print(f"   MAE:      {mae:,.0f} € ({mae_pct:.2f}% del valor)")
    print(f"   RMSE:     {rmse:,.0f} €")
    print(f"   R²:       {r2:.4f}")

    importancia = pd.DataFrame({
        'feature': features_disponibles,
        'importancia': modelo.feature_importances_
    }).sort_values('importancia', ascending=False)

    print(f"\n Top 10 features más importantes:")
    print(importancia.head(10).to_string(index=False))

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
    titulo_base = f'Modelo XGBoost — Horizonte {horizonte} día(s)\nMAE: {mae:,.0f}€ ({mae_pct:.2f}%)  |  R²: {r2:.4f}'

    # --- Gráfico 1: Real vs Predicho ---
    fig, ax = plt.subplots(figsize=(7, 6))
    fig.suptitle(titulo_base, fontsize=12, fontweight='bold')
    lim = max(abs(y_test.max()), abs(y_test.min())) * 1.1
    ax.scatter(y_test, y_pred, alpha=0.3, s=5, color='steelblue')
    ax.plot([-lim, lim], [-lim, lim], 'r--', lw=1.5, label='Predicción perfecta')
    ax.set_xlabel('Variación Real (€)')
    ax.set_ylabel('Variación Predicha (€)')
    ax.set_title('Real vs Predicho')
    ax.legend()
    ax.set_xlim(-lim, lim)
    ax.set_ylim(-lim, lim)
    plt.tight_layout()
    ruta1 = os.path.join(OUTPUT_DIR, f'dispersión_{horizonte}d.png')
    plt.savefig(ruta1, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"    Gráfico guardado: {ruta1}")

    # --- Gráfico 2: Distribución del error ---
    fig, ax = plt.subplots(figsize=(7, 6))
    fig.suptitle(titulo_base, fontsize=12, fontweight='bold')
    errores = y_pred - y_test.values
    ax.hist(errores, bins=60, color='steelblue', edgecolor='white', alpha=0.8)
    ax.axvline(0, color='red', lw=1.5, linestyle='--')
    ax.set_xlabel('Error (€)')
    ax.set_ylabel('Frecuencia')
    ax.set_title('Distribución del Error')
    plt.tight_layout()
    ruta2 = os.path.join(OUTPUT_DIR, f'error_{horizonte}d.png')
    plt.savefig(ruta2, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"    Gráfico guardado: {ruta2}")

    # --- Gráfico 3: Importancia de variables ---
    fig, ax = plt.subplots(figsize=(9, 6))
    top15 = importancia.head(15).copy()
    top15['label'] = top15['feature'].map(ETIQUETAS_FEATURES).fillna(top15['feature'])
    ax.barh(top15['label'][::-1], top15['importancia'][::-1], color='steelblue')
    ax.set_xlabel('Importancia')
    ax.set_title(f'Top 15 variables más influyentes — Horizonte {horizonte} día(s)')
    ax.tick_params(axis='y', labelsize=9)
    plt.tight_layout()
    ruta3 = os.path.join(OUTPUT_DIR, f'importancia_{horizonte}d.png')
    plt.savefig(ruta3, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"    Gráfico guardado: {ruta3}")

# ─────────────────────────────────────────────
# 6. PREDICCIÓN DE JUGADORES ESPECÍFICOS
# ─────────────────────────────────────────────
def predecir_jugadores(df, resultados, top_n=20):
    """Genera una tabla con predicciones para los últimos datos disponibles."""
    print(f"\n{'='*50}")
    print(" PREDICCIONES PARA MAÑANA (último día disponible)")
    print(f"{'='*50}")

    ultimo_dia = df['date'].max()
    df_hoy = df[df['date'] == ultimo_dia].copy()

    print(f"   Fecha base: {ultimo_dia.date()}")
    print(f"   Jugadores con datos: {len(df_hoy)}")

    resultado_1d = resultados[1]
    modelo = resultado_1d['modelo']
    features = resultado_1d['features']

    features_ok = [f for f in features if f in df_hoy.columns]
    df_pred = limpiar_X(df_hoy[features_ok].fillna(0))

    predicciones = modelo.predict(df_pred)
    df_hoy = df_hoy.copy()
    df_hoy['pred_var_1d'] = predicciones
    df_hoy['pred_mv_mañana'] = df_hoy['marketValue'] + predicciones
    df_hoy['pred_pct_1d'] = (predicciones / df_hoy['marketValue'] * 100).round(2)

    # Top subidas y bajadas
    tabla = df_hoy[['player_name', 'posicion', 'equipo', 'marketValue',
                     'pred_var_1d', 'pred_pct_1d', 'pred_mv_mañana']].copy()
    tabla = tabla.sort_values('pred_var_1d', ascending=False)

    print(f"\n TOP {top_n} SUBIDAS PREDICHAS:")
    print(tabla.head(top_n).to_string(index=False))

    print(f"\n TOP {top_n} BAJADAS PREDICHAS:")
    print(tabla.tail(top_n).to_string(index=False))

    # Guardar CSV
    ruta_csv = os.path.join(OUTPUT_DIR, 'predicciones_mañana.csv')
    tabla.to_csv(ruta_csv, index=False)
    print(f"\n Predicciones guardadas: {ruta_csv}")

    return tabla


# ─────────────────────────────────────────────
# 7. RESUMEN FINAL
# ─────────────────────────────────────────────
def resumen_final(resultados):
    print(f"\n{'='*50}")
    print(" RESUMEN COMPARATIVO DE MODELOS")
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
    print(f"\n Resumen guardado: {ruta}")


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

    print(f"\n ¡Todo listo! Resultados en la carpeta: {OUTPUT_DIR}/")