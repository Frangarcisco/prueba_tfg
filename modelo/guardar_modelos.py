"""
guardar_modelos.py
==================
Script de uso único: entrena los tres modelos XGBoost (1d, 3d, 7d)
con el split automático de 30 días y los guarda en disco.

Ejecutar UNA SOLA VEZ antes de lanzar la aplicación:
    python .\\modelo\\guardar_modelos.py   (desde la raíz del proyecto)
"""

import os
import sys
import joblib
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from modelo_tfg import crear_features, FEATURES, HORIZONTES

BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATASET_PATH = os.path.join(BASE_DIR, "data", "raw", "DATASET_MAESTRO_TFG_corregido.csv")
OUTPUT_DIR   = os.path.join(BASE_DIR, "modelos_guardados")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def limpiar_X(df_x):
    """
    Convierte todas las columnas a float64 de forma robusta.
    Resuelve object, bool, UInt32 y cualquier tipo que XGBoost no acepte.
    """
    resultado = pd.DataFrame(index=df_x.index)
    for col in df_x.columns:
        serie = df_x[col]
        # Si la columna contiene objetos compuestos (DataFrames anidados, etc.)
        # los aplana a escalar primero
        if hasattr(serie, 'values') and hasattr(serie.values, 'dtype'):
            try:
                resultado[col] = pd.to_numeric(serie, errors='coerce').fillna(0).astype('float64')
            except Exception:
                resultado[col] = 0.0
        else:
            resultado[col] = 0.0
    return resultado


def cargar_datos_app(path):
    print(f"  Cargando dataset desde: {path}")
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['date'])
    df = df.sort_values(['player_id', 'date']).reset_index(drop=True)
    df.drop(columns=['bids'], inplace=True, errors='ignore')
    df['equipo']   = df.groupby('player_id')['equipo'].ffill().bfill()
    df['nombre']   = df.groupby('player_id')['nombre'].ffill().bfill()
    df['posicion'] = df.groupby('player_id')['posicion'].ffill().bfill()
    cols_num = df.select_dtypes(include=[np.number]).columns
    df[cols_num] = df[cols_num].fillna(0)
    print(f"  Dataset cargado: {df.shape[0]:,} filas, {df.shape[1]} columnas")
    print(f"  Jugadores: {df['player_id'].nunique()}")
    print(f"  Rango fechas: {df['date'].min().date()} -> {df['date'].max().date()}")
    return df


def entrenar_y_guardar(df, horizonte):
    print(f"\n{'='*50}")
    print(f"  Entrenando modelo -- horizonte {horizonte} dia(s)")
    print(f"{'='*50}")

    target = f"target_{horizonte}d"
    features_disponibles = [f for f in FEATURES if f in df.columns]

    cols_necesarias = features_disponibles + [target, "date", "player_id", "marketValue"]
    cols_presentes  = [c for c in cols_necesarias if c in df.columns]
    df_modelo = df[cols_presentes].dropna(subset=[target] + features_disponibles)

    X      = df_modelo[features_disponibles]
    y      = df_modelo[target]
    fechas = df_modelo["date"]

    fecha_corte = fechas.max() - pd.Timedelta(days=30)
    train_mask  = fechas <= fecha_corte
    test_mask   = fechas >  fecha_corte

    # Limpiar tipos antes de pasar a XGBoost
    X_train = limpiar_X(X[train_mask])
    X_test  = limpiar_X(X[test_mask])
    y_train = y[train_mask]
    y_test  = y[test_mask]

    print(f"  Train: {len(X_train):,} filas hasta {fecha_corte.date()}")
    print(f"  Test:  {len(X_test):,} filas")

    if len(X_test) == 0:
        raise ValueError(f"Test vacio para horizonte {horizonte}d.")

    modelo = xgb.XGBRegressor(
        n_estimators          = 500,
        learning_rate         = 0.05,
        max_depth             = 6,
        subsample             = 0.8,
        colsample_bytree      = 0.8,
        min_child_weight      = 5,
        reg_alpha             = 0.1,
        reg_lambda            = 1.0,
        random_state          = 42,
        n_jobs                = -1,
        early_stopping_rounds = 30,
        eval_metric           = "mae",
    )

    modelo.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    y_pred = modelo.predict(X_test)
    mae    = mean_absolute_error(y_test, y_pred)
    rmse   = np.sqrt(mean_squared_error(y_test, y_pred))
    r2     = r2_score(y_test, y_pred)

    print(f"\n  Resultados:")
    print(f"    MAE:  {mae:,.0f} euros")
    print(f"    RMSE: {rmse:,.0f} euros")
    print(f"    R2:   {r2:.4f}")

    ruta = os.path.join(OUTPUT_DIR, f"modelo_{horizonte}d.pkl")
    joblib.dump({
        "modelo":      modelo,
        "features":    features_disponibles,
        "horizonte":   horizonte,
        "metricas":    {"mae": mae, "rmse": rmse, "r2": r2},
        "fecha_corte": fecha_corte,
    }, ruta)
    print(f"  Guardado en: {ruta}")


if __name__ == "__main__":
    print("=" * 50)
    print("  GUARDADO DE MODELOS -- LaLiga Fantasy TFG")
    print("=" * 50)

    df = cargar_datos_app(DATASET_PATH)
    df = crear_features(df)

    for h in HORIZONTES:
        entrenar_y_guardar(df, h)

    print(f"\n{'='*50}")
    print(f"  Listo! Modelos guardados en: {OUTPUT_DIR}")
    for h in HORIZONTES:
        print(f"    - modelo_{h}d.pkl")
    print(f"\n  Ahora puedes lanzar la app con:")
    print(f"    streamlit run app.py")
    print("=" * 50)