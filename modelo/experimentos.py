# experimentos.py
import pandas as pd
import numpy as np
import os, sys
sys.path.insert(0, '.')

from modelo.modelo_tfg import cargar_datos, crear_features, entrenar_modelo, FEATURES
from modelo.baselines import calcular_baselines

def generar_splits_expanding(df, meses_minimos=2, paso_meses=1):
    """
    Expanding window: el entrenamiento siempre empieza en el día 1,
    la ventana de test avanza mes a mes.
    
    Devuelve lista de dicts con keys: mes_test, train_mask, test_mask, label
    """
    fecha_min = df['date'].min()
    fecha_max = df['date'].max()
    
    splits = []
    fecha_corte = fecha_min + pd.DateOffset(months=meses_minimos)
    
    while fecha_corte + pd.DateOffset(months=paso_meses) <= fecha_max:
        fecha_fin_test = fecha_corte + pd.DateOffset(months=paso_meses)
        
        train_mask = df['date'] < fecha_corte
        test_mask  = (df['date'] >= fecha_corte) & (df['date'] < fecha_fin_test)
        
        if train_mask.sum() < 100 or test_mask.sum() < 30:
            fecha_corte += pd.DateOffset(months=paso_meses)
            continue
        
        splits.append({
            'label': f"train_hasta_{fecha_corte.strftime('%Y-%m')}_test_{fecha_fin_test.strftime('%Y-%m')}",
            'mes_test': fecha_corte.strftime('%Y-%m'),
            'train_mask': train_mask,
            'test_mask': test_mask,
            'n_train': train_mask.sum(),
            'n_test': test_mask.sum(),
        })
        fecha_corte += pd.DateOffset(months=paso_meses)
    
    print(f"  Expanding window: {len(splits)} splits generados")
    return splits


def generar_splits_rolling(df, ventana_dias=90, paso_dias=30):
    """
    Rolling window: ventana de entrenamiento de tamaño fijo,
    se desliza hacia adelante.
    """
    fecha_min = df['date'].min()
    fecha_max = df['date'].max()
    
    splits = []
    inicio_train = fecha_min
    
    while True:
        fin_train  = inicio_train + pd.Timedelta(days=ventana_dias)
        fin_test   = fin_train    + pd.Timedelta(days=paso_dias)
        
        if fin_test > fecha_max:
            break
        
        train_mask = (df['date'] >= inicio_train) & (df['date'] < fin_train)
        test_mask  = (df['date'] >= fin_train)    & (df['date'] < fin_test)
        
        if train_mask.sum() < 100 or test_mask.sum() < 30:
            inicio_train += pd.Timedelta(days=paso_dias)
            continue
        
        splits.append({
            'label': f"train_{inicio_train.strftime('%Y-%m-%d')}_{fin_train.strftime('%Y-%m-%d')}",
            'inicio_train': inicio_train.strftime('%Y-%m-%d'),
            'fin_train': fin_train.strftime('%Y-%m-%d'),
            'fin_test': fin_test.strftime('%Y-%m-%d'),
            'train_mask': train_mask,
            'test_mask': test_mask,
            'n_train': train_mask.sum(),
            'n_test': test_mask.sum(),
        })
        inicio_train += pd.Timedelta(days=paso_dias)
    
    print(f"  Rolling window ({ventana_dias}d train / {paso_dias}d paso): {len(splits)} splits")
    return splits


def ejecutar_experimento(df, splits, horizonte, nombre_exp, output_dir):
    """
    Itera sobre splits, llama a entrenar_modelo() con cada mask,
    recoge métricas y guarda resumen.
    """
    os.makedirs(output_dir, exist_ok=True)
    filas = []
    
    for i, split in enumerate(splits):
        print(f"\n  Split {i+1}/{len(splits)}: {split['label']}")
        
        try:
            _, res = entrenar_modelo(
                df, horizonte,
                train_mask=split['train_mask'],
                test_mask=split['test_mask']
            )
            fila = {
                'split': split['label'],
                'n_train': split['n_train'],
                'n_test': split['n_test'],
                'mae': res['mae'],
                'mae_pct': res['mae_pct'],
                'rmse': res['rmse'],
                'r2': res['r2'],
            }
            # Añadir metadatos específicos del split
            for k in ['mes_test', 'inicio_train', 'fin_train', 'fin_test']:
                if k in split:
                    fila[k] = split[k]
            filas.append(fila)
        except Exception as e:
            print(f"    Split fallido: {e}")
            continue
    
    df_res = pd.DataFrame(filas)
    ruta = os.path.join(output_dir, f'{nombre_exp}_{horizonte}d.csv')
    df_res.to_csv(ruta, index=False)
    print(f"\n  Resultados guardados: {ruta}")
    return df_res