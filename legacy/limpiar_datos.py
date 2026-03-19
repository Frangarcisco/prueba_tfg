import pandas as pd
import ast
import os

def procesar_dataset_fantasy(input_file, output_file):
    print(f"📖 Leyendo archivo: {input_file}...")
    
    if not os.path.exists(input_file):
        print(f"❌ ERROR: No encuentro el archivo '{input_file}'")
        return

    df = pd.read_csv(input_file)
    filas_procesadas = []

    print("⚙️ Procesando y corrigiendo estadísticas...")
    
    for index, row in df.iterrows():
        try:
            if pd.isna(row['playerStats']) or row['playerStats'] == '[]':
                continue
                
            stats_list = ast.literal_eval(row['playerStats'])
            
            for semana in stats_list:
                # 1. Datos básicos
                nueva_fila = {
                    'player_id': row['id'],
                    'nombre': row['nickname'],
                    'posicion': row['positionId'],
                    'equipo': row.get('team.name', 'Desconocido'),
                    'jornada': semana.get('weekNumber'),
                    'puntos_totales': semana.get('totalPoints'),
                    'en_once_ideal': semana.get('isInIdealFormation', False)
                }

                # 2. Desglose de estadísticas (CON LA CORRECCIÓN)
                detalles = semana.get('stats', {})
                
                for stat_nombre, valores in detalles.items():
                    if isinstance(valores, list) and len(valores) >= 1:
                        
                        # --- AQUÍ ESTÁ EL CAMBIO ---
                        if stat_nombre == 'marca_points':
                            # Para los puntos Marca, el valor real es el SEGUNDO (índice 1)
                            # El primero suele ser -1 (código de proveedor)
                            if len(valores) > 1:
                                nueva_fila[stat_nombre] = valores[1]
                            else:
                                nueva_fila[stat_nombre] = 0
                        else:
                            # Para todo lo demás (goles, minutos...), el valor real es el PRIMERO (índice 0)
                            nueva_fila[stat_nombre] = valores[0]
                        
                filas_procesadas.append(nueva_fila)
                
        except Exception as e:
            # Si falla una fila puntual, la saltamos pero no paramos todo
            continue

    # Crear DataFrame
    df_final = pd.DataFrame(filas_procesadas)
    
    # Limpieza final: Rellenar huecos con 0 y asegurar enteros
    df_final = df_final.fillna(0)
    
    # Guardar
    df_final.to_csv(output_file, index=False)
    
    print(f"\n✅ ¡CORREGIDO! Nuevo archivo generado: {output_file}")
    print(f"📊 Filas totales: {len(df_final)}")
    
    # Verificación rápida para que te quedes tranquilo
    if 'marca_points' in df_final.columns:
        print("\n🔍 Muestra de Puntos Marca (ya no deberían ser -1):")
        # Mostramos solo filas donde haya puntos marca distintos de 0 para verificar
        print(df_final[df_final['marca_points'] != 0][['nombre', 'jornada', 'marca_points']].head(5))
    else:
        print("⚠️ Ojo: No veo la columna 'marca_points', revisa si tus jugadores tienen ese dato.")

# --- EJECUCIÓN ---
if __name__ == "__main__":
    ARCHIVO_ENTRADA = "dataset_stats_jugadores_20260209.csv" # Tu archivo original
    ARCHIVO_SALIDA = "dataset_entrenamiento_FINAL.csv"
    
    procesar_dataset_fantasy(ARCHIVO_ENTRADA, ARCHIVO_SALIDA)