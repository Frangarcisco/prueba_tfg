"""
data_processing/clean_stats.py
================================
Limpieza y normalización del dataset de estadísticas por jornada.

El fichero crudo (dataset_stats_jugadores.csv) contiene la columna
'playerStats' como una lista de diccionarios serializada en texto.
Este módulo la desanida jornada a jornada, aplica la corrección del
índice de 'marca_points' (cuyo valor real está en la posición 1 de
la lista, no en la 0) y genera un CSV limpio listo para el modelado.

La función puede importarse desde build_master_dataset.py o ejecutarse
de forma independiente.
"""

import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from config import STATS_RAW_FILE, STATS_CLEAN_FILE, PROCESSED_DIR


# Estadísticas cuyo valor real está en el SEGUNDO elemento de la lista.
# El resto usan el PRIMER elemento.
STATS_SECOND_INDEX = {"marca_points"}


def parse_player_stats(row: pd.Series) -> list:
    """
    Desanida la columna 'playerStats' de una fila del CSV crudo.

    Parameters
    ----------
    row : pd.Series
        Fila del DataFrame original con los campos del jugador y su
        columna 'playerStats' como string serializado.

    Returns
    -------
    list[dict]
        Lista de registros, uno por jornada jugada por el jugador.
        Devuelve lista vacía si 'playerStats' está ausente o vacío.
    """
    raw_stats = row.get("playerStats")
    if pd.isna(raw_stats) or raw_stats == "[]":
        return []

    try:
        stats_list = ast.literal_eval(raw_stats)
    except (ValueError, SyntaxError):
        return []

    records = []
    for week in stats_list:
        record = {
            "player_id":      row["id"],
            "nombre":         row["nickname"],
            "posicion":       row["positionId"],
            "equipo":         row.get("team.name", "Desconocido"),
            "jornada":        week.get("weekNumber"),
            "puntos_totales": week.get("totalPoints"),
            "en_once_ideal":  week.get("isInIdealFormation", False),
        }

        # Desanidar estadísticas individuales con corrección de índice
        for stat_name, values in week.get("stats", {}).items():
            if not isinstance(values, list) or len(values) == 0:
                continue
            if stat_name in STATS_SECOND_INDEX:
                record[stat_name] = values[1] if len(values) > 1 else 0
            else:
                record[stat_name] = values[0]

        records.append(record)

    return records


def clean_stats(input_path=None, output_path=None) -> pd.DataFrame:
    """
    Ejecuta el pipeline completo de limpieza de estadísticas.

    Lee el CSV crudo, desanida las estadísticas jornada a jornada,
    aplica las correcciones necesarias y guarda el resultado.

    Parameters
    ----------
    input_path : Path or str, optional
        Ruta al CSV crudo. Por defecto usa STATS_RAW_FILE de config.py.
    output_path : Path or str, optional
        Ruta de salida. Por defecto usa STATS_CLEAN_FILE de config.py.

    Returns
    -------
    pd.DataFrame
        Dataset limpio con una fila por jugador y jornada.
    """
    input_path  = Path(input_path  or STATS_RAW_FILE)
    output_path = Path(output_path or STATS_CLEAN_FILE)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print(f"  📖 Leyendo estadísticas crudas: {input_path.name}")
    df_raw = pd.read_csv(input_path)

    print(f"  ⚙️  Desanidando jornadas ({len(df_raw)} jugadores)...")
    all_records = []
    for _, row in df_raw.iterrows():
        all_records.extend(parse_player_stats(row))

    df_clean = pd.DataFrame(all_records).fillna(0)
    df_clean.to_csv(output_path, index=False)

    print(f"  ✅ Estadísticas limpias: {output_path.name}  ({len(df_clean):,} filas)")
    return df_clean


if __name__ == "__main__":
    clean_stats()
