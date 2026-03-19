import pandas as pd
import numpy as np

def cargar_y_preparar_datos():
    print("📂 Cargando los 3 datasets...")
    
    # 1. Cargar MERCADO (Diario)
    df_mercado = pd.read_csv("dataset_mercado_20260209.csv") 
    df_mercado['date'] = pd.to_datetime(df_mercado['date'], utc=True).dt.tz_localize(None)
    df_mercado.sort_values(['player_id', 'date'], inplace=True)
    
    # Aseguramos nombres consistentes
    if 'id' in df_mercado.columns: df_mercado.rename(columns={'id': 'player_id'}, inplace=True)
    
    # 2. Cargar STATS (Por Jornada)
    df_stats = pd.read_csv("dataset_entrenamiento_FINAL.csv")
    if 'id' in df_stats.columns: df_stats.rename(columns={'id': 'player_id'}, inplace=True)
    
    # 3. Cargar CALENDARIO (Fechas de partidos)
    df_calendario = pd.read_csv("calendario_laliga_2025.csv")
    df_calendario['fecha_partido'] = pd.to_datetime(df_calendario['fecha_partido'], utc=True).dt.tz_localize(None)
    df_calendario.sort_values('fecha_partido', inplace=True)

    return df_mercado, df_stats, df_calendario

def enriquecer_stats(df_stats):
    print("⚙️ Calculando medias acumuladas y rankings...")
    df_stats.sort_values(['player_id', 'jornada'], inplace=True)
    
    # --- VARIABLES ACUMULADAS ---
    df_stats['puntos_acumulados'] = df_stats.groupby('player_id')['puntos_totales'].cumsum()
    df_stats['partidos_jugados'] = df_stats.groupby('player_id').cumcount() + 1
    df_stats['media_puntos_hasta_fecha'] = df_stats['puntos_acumulados'] / df_stats['partidos_jugados']
    
    if 'goals' in df_stats.columns:
        df_stats['goles_acumulados'] = df_stats.groupby('player_id')['goals'].cumsum()
    
    # Ranking por Posición
    df_stats['ranking_posicion_jornada'] = df_stats.groupby(['jornada', 'posicion'])['puntos_totales'].rank(ascending=False, method='min')
    
    return df_stats

def cruzar_todo(df_mercado, df_stats, df_calendario):
    print("🔄 Cruzando Mercado (Diario) con Stats y Calendario...")
    
    # 1. Asignar FECHA a cada Jornada
    fecha_jornadas = df_calendario.groupby('jornada')['fecha_partido'].min().reset_index()
    fecha_jornadas.rename(columns={'fecha_partido': 'fecha_jornada'}, inplace=True)
    
    df_stats = pd.merge(df_stats, fecha_jornadas, on='jornada', how='left')
    df_stats['fecha_jornada'] = pd.to_datetime(df_stats['fecha_jornada'])

    # 2. CRUZAR MERCADO CON STATS (Merge As Of - Backward)
    df_mercado = df_mercado.sort_values('date')
    df_stats = df_stats.sort_values('fecha_jornada')
    
    df_master = pd.merge_asof(
        df_mercado, 
        df_stats, 
        left_on='date', 
        right_on='fecha_jornada', 
        by='player_id', 
        direction='backward' 
    )
    
    # 3. VARIABLES DE CALENDARIO
    print("⏳ Calculando días para el próximo partido...")
    df_calendario = df_calendario.sort_values('fecha_partido')
    
    # Próximo partido
    df_master = pd.merge_asof(
        df_master,
        df_calendario[['fecha_partido', 'jornada']].rename(columns={'fecha_partido': 'fecha_siguiente_partido', 'jornada': 'prox_jornada'}),
        left_on='date',
        right_on='fecha_siguiente_partido',
        direction='forward'
    )
    
    # Cálculos finales
    df_master['dias_prox_partido'] = (df_master['fecha_siguiente_partido'] - df_master['date']).dt.days
    df_master['is_paron_selecciones'] = df_master['dias_prox_partido'] > 12
    
    fecha_inicio = df_master['date'].min()
    df_master['dias_desde_inicio'] = (df_master['date'] - fecha_inicio).dt.days
    
    return df_master

# --- EJECUCIÓN ---
if __name__ == "__main__":
    try:
        df_m, df_s, df_c = cargar_y_preparar_datos()
        df_s_enriquecido = enriquecer_stats(df_s)
        df_final = cruzar_todo(df_m, df_s_enriquecido, df_c)
        
        # --- LIMPIEZA FINAL (Esto arregla lo "corrupto") ---
        # Rellenamos los NaNs con 0 (para pretemporada o datos faltantes)
        columnas_numericas = df_final.select_dtypes(include=[np.number]).columns
        df_final[columnas_numericas] = df_final[columnas_numericas].fillna(0)
        
        # Guardar
        nombre_archivo = "DATASET_MAESTRO_TFG.csv"
        df_final.to_csv(nombre_archivo, index=False)
        
        print(f"\n🚀 ¡EXITAZO! Dataset Maestro generado: {nombre_archivo}")
        print(f"📊 Filas totales: {len(df_final)}")
        
        # Previsualización CORREGIDA (usando 'player_name' en vez de 'nickname')
        print("\nPrevisualización (Primeras 5 filas):")
        cols_to_show = ['date', 'player_name', 'marketValue', 'dias_prox_partido', 'media_puntos_hasta_fecha']
        # Filtramos columnas que realmente existan para evitar errores
        cols_existing = [c for c in cols_to_show if c in df_final.columns]
        print(df_final[cols_existing].head(5))
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("Revisa los nombres de las columnas en tus CSVs.")