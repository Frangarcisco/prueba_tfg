import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
import os

def calcular_baselines(df_train, df_test, horizonte, output_dir="resultados/baseline"):
    """
    Calcula tres baselines sobre df_test:
      - Zero: predice variación = 0 siempre
      - Naive lag-1: predice la variación del día anterior
      - MA7: media móvil de variación de los últimos 7 días
    
    df_train y df_test deben tener columnas: player_id, date, marketValue, target_{h}d
    Devuelve dict con métricas de cada baseline.
    """
    os.makedirs(output_dir, exist_ok=True)
    target_col = f'target_{horizonte}d'
    
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
        'descripcion': 'Predice variación = 0 siempre'
    }
    
    # ── Baseline 2: Naive lag-1 ───────────────────────────
    # variación del día anterior por jugador
    df_full = pd.concat([df_train, df_test]).sort_values(['player_id', 'date'])
    df_full['var_ayer'] = df_full.groupby('player_id')['marketValue'].diff(1)
    
    df_test_merged = df_test.copy()
    df_test_merged = df_test_merged.merge(
        df_full[['player_id', 'date', 'var_ayer']],
        on=['player_id', 'date'], how='left'
    )
    
    mask_valid = df_test_merged[target_col].notna() & df_test_merged['var_ayer'].notna()
    y_true_lag = df_test_merged.loc[mask_valid, target_col]
    pred_lag1  = df_test_merged.loc[mask_valid, 'var_ayer']
    mv_lag     = df_test_merged.loc[mask_valid, 'marketValue']
    
    mae_lag1 = mean_absolute_error(y_true_lag, pred_lag1)
    mae_pct_lag1 = (np.abs(y_true_lag.values - pred_lag1.values) / mv_lag.values * 100).mean()
    
    resultados['naive_lag1'] = {
        'mae': mae_lag1,
        'mae_pct': mae_pct_lag1,
        'descripcion': 'Predice la variación del día anterior'
    }
    
    # ── Baseline 3: Moving Average 7 días ─────────────────
    df_full['var_diaria'] = df_full.groupby('player_id')['marketValue'].diff(1)
    df_full['ma7_var'] = (
        df_full.groupby('player_id')['var_diaria']
        .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
    )
    
    df_test_ma = df_test.copy()
    df_test_ma = df_test_ma.merge(
        df_full[['player_id', 'date', 'ma7_var']],
        on=['player_id', 'date'], how='left'
    )
    
    mask_ma = df_test_ma[target_col].notna() & df_test_ma['ma7_var'].notna()
    y_true_ma = df_test_ma.loc[mask_ma, target_col]
    pred_ma7  = df_test_ma.loc[mask_ma, 'ma7_var']
    mv_ma     = df_test_ma.loc[mask_ma, 'marketValue']
    
    mae_ma7 = mean_absolute_error(y_true_ma, pred_ma7)
    mae_pct_ma7 = (np.abs(y_true_ma.values - pred_ma7.values) / mv_ma.values * 100).mean()
    
    resultados['ma7'] = {
        'mae': mae_ma7,
        'mae_pct': mae_pct_ma7,
        'descripcion': 'Media móvil de variación (7 días)'
    }
    
    # ── Guardar resumen ───────────────────────────────────
    filas = [
        {'baseline': k, **{kk: vv for kk, vv in v.items()}}
        for k, v in resultados.items()
    ]
    pd.DataFrame(filas).to_csv(
        os.path.join(output_dir, f'baselines_{horizonte}d.csv'), index=False
    )
    
    print(f"\n  BASELINES — Horizonte {horizonte}d:")
    for nombre, m in resultados.items():
        print(f"   {nombre:12s}  MAE: {m['mae']:>10,.0f}€  ({m['mae_pct']:.2f}%)")
    
    return resultados

if __name__ == "__main__":
    print("Ejecutando baselines...")

    from modelo_tfg import cargar_datos, crear_features

    df = cargar_datos("data/raw/DATASET_MAESTRO_TFG.csv")
    df = crear_features(df)

    import pandas as pd
    fecha_corte = df['date'].max() - pd.Timedelta(days=30)

    df_train = df[df['date'] <= fecha_corte]
    df_test  = df[df['date'] >  fecha_corte]

    calcular_baselines(df_train, df_test, horizonte=1)