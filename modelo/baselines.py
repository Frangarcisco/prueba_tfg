import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
import os

from modelo_tfg import cargar_datos, crear_features, FEATURES, HORIZONTES


def obtener_filas_alineadas(df, horizonte):
    """
    Reproduce exactamente el mismo dropna que usa modelo_tfg.py en
    entrenar_modelo() para este horizonte, y devuelve el conjunto
    (player_id, date) de las filas de TEST que XGBoost usó realmente.
    Esto es lo que garantiza que los baselines se evalúen sobre la
    misma población de filas que el modelo XGBoost.
    """
    target = f'target_{horizonte}d'
    features_disponibles = [f for f in FEATURES if f in df.columns]

    df_modelo = df[features_disponibles + [target, 'date', 'player_id', 'player_name']].dropna()

    fecha_corte = df_modelo['date'].max() - pd.Timedelta(days=30)
    test_mask = df_modelo['date'] > fecha_corte

    return df_modelo.loc[test_mask, ['player_id', 'date']].copy()


def calcular_baselines(df_train, df_test, horizonte, filas_alineadas=None, output_dir="resultados/baseline"):
    """
    Calcula tres baselines sobre df_test:
      - Zero: predice variación = 0 siempre
      - Naive lag-1: predice la variación del día anterior
      - MA7: media móvil de variación de los últimos 7 días

    Si se pasa `filas_alineadas`, df_test se recorta primero a esas filas
    exactas (player_id, date), garantizando que los tres baselines y el
    modelo XGBoost se evalúan sobre el mismo conjunto de test.
    """
    os.makedirs(output_dir, exist_ok=True)
    target_col = f'target_{horizonte}d'

    # ── Alinear df_test con las filas usadas por XGBoost ──
    if filas_alineadas is not None:
        n_antes = len(df_test)
        df_test = df_test.merge(filas_alineadas, on=['player_id', 'date'], how='inner')
        print(f"   Filas de test alineadas con XGBoost: {len(df_test)} (antes: {n_antes})")

    resultados = {}

    # ── Baseline 1: Zero ──────────────────────────────────
    y_test = df_test[target_col].dropna()
    pred_zero = np.zeros(len(y_test))
    mae_zero = mean_absolute_error(y_test, pred_zero)
    mv_test = df_test.loc[y_test.index, 'marketValue']
    mae_pct_zero = (np.abs(y_test.values - pred_zero) / mv_test.values * 100).mean()

    resultados['zero'] = {
        'mae': mae_zero,
        'mae_pct': mae_pct_zero,
        'n': len(y_test),
        'descripcion': 'Predice variación = 0 siempre'
    }

# ── Baseline 2: Naive lag-h (persistencia a la escala del horizonte) ──
    df_full = df_train.sort_values(['player_id', 'date']).copy()
    df_full['var_pasado_h'] = df_full.groupby('player_id')['marketValue'].diff(horizonte)

    df_test_merged = df_test.copy()
    df_test_merged = df_test_merged.merge(
        df_full[['player_id', 'date', 'var_pasado_h']],
        on=['player_id', 'date'], how='left'
    )

    mask_valid = df_test_merged[target_col].notna() & df_test_merged['var_pasado_h'].notna()
    y_true_lag = df_test_merged.loc[mask_valid, target_col]
    pred_lagh  = df_test_merged.loc[mask_valid, 'var_pasado_h']
    mv_lag     = df_test_merged.loc[mask_valid, 'marketValue']

    mae_lagh = mean_absolute_error(y_true_lag, pred_lagh)
    mae_pct_lagh = (np.abs(y_true_lag.values - pred_lagh.values) / mv_lag.values * 100).mean()

    resultados[f'naive_lag{horizonte}'] = {
        'mae': mae_lagh,
        'mae_pct': mae_pct_lagh,
        'n': len(y_true_lag),
        'descripcion': f'Predice que la variación a {horizonte} día(s) será igual a la variación observada en los {horizonte} día(s) anteriores'
    }

    # ── Baseline 3: Moving Average 7 días (escalada al horizonte) ─────────
    df_full['var_diaria'] = df_full.groupby('player_id')['marketValue'].diff(1)
    df_full['ma7_var_diaria'] = (
        df_full.groupby('player_id')['var_diaria']
        .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
    )
    # Extrapola la tendencia diaria media a la escala del horizonte evaluado
    df_full['ma7_var_h'] = df_full['ma7_var_diaria'] * horizonte

    df_test_ma = df_test.copy()
    df_test_ma = df_test_ma.merge(
        df_full[['player_id', 'date', 'ma7_var_h']],
        on=['player_id', 'date'], how='left'
    )

    mask_ma = df_test_ma[target_col].notna() & df_test_ma['ma7_var_h'].notna()
    y_true_ma = df_test_ma.loc[mask_ma, target_col]
    pred_ma7  = df_test_ma.loc[mask_ma, 'ma7_var_h']
    mv_ma     = df_test_ma.loc[mask_ma, 'marketValue']

    mae_ma7 = mean_absolute_error(y_true_ma, pred_ma7)
    mae_pct_ma7 = (np.abs(y_true_ma.values - pred_ma7.values) / mv_ma.values * 100).mean()

    resultados['ma7'] = {
        'mae': mae_ma7,
        'mae_pct': mae_pct_ma7,
        'n': len(y_true_ma),
        'descripcion': f'Extrapola la media móvil de variación diaria (7 días) a {horizonte} día(s)'
    }

    # ── Guardar resumen ───────────────────────────────────
    filas = [
        {'baseline': k, **{kk: vv for kk, vv in v.items()}}
        for k, v in resultados.items()
    ]
    pd.DataFrame(filas).to_csv(
        os.path.join(output_dir, f'baselines_{horizonte}d.csv'), index=False
    )

    print(f"\n  BASELINES — Horizonte {horizonte}d (alineado con XGBoost):")
    for nombre, m in resultados.items():
        print(f"   {nombre:12s}  MAE: {m['mae']:>10,.0f}€  ({m['mae_pct']:.2f}%)  n={m['n']}")

    return resultados


if __name__ == "__main__":
    print("Ejecutando baselines alineados con XGBoost...")

    df = cargar_datos("data/raw/DATASET_MAESTRO_TFG_corregido.csv")
    df = crear_features(df)

    for h in HORIZONTES:
        print(f"\n{'='*50}\nHorizonte: {h} día(s)\n{'='*50}")
        filas_alineadas = obtener_filas_alineadas(df, h)
        calcular_baselines(df, df, horizonte=h, filas_alineadas=filas_alineadas)