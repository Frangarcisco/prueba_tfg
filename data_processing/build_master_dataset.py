"""
data_processing/build_master_dataset.py
=========================================
Generación del Dataset Maestro para el modelado predictivo (TFG).

Integra las tres fuentes de datos descargadas en data/raw/ y calcula
las variables derivadas necesarias para el modelo de predicción del
valor de mercado:

  - Media de puntos acumulada hasta cada fecha (sin data leakage).
  - Goles acumulados en la temporada.
  - Ranking del jugador en su posición por jornada.
  - Días hasta el próximo partido.
  - Indicador de parón de selecciones.
  - Días transcurridos desde el inicio de la temporada.

El fichero resultante (DATASET_MAESTRO_TFG.csv) es la entrada directa
del pipeline de modelado.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np

from config import (
    MARKET_HISTORY_FILE,
    STATS_CLEAN_FILE,
    CALENDAR_FILE,
    MASTER_DATASET_FILE,
    PROCESSED_DIR,
    PARON_DIAS_UMBRAL,
)


# =============================================================================
# CARGA
# =============================================================================

def load_market() -> pd.DataFrame:
    """
    Carga el historial de valor de mercado y normaliza fechas.

    Returns
    -------
    pd.DataFrame
        DataFrame ordenado por jugador y fecha.
    """
    df = pd.read_csv(MARKET_HISTORY_FILE)
    if "id" in df.columns:
        df.rename(columns={"id": "player_id"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)
    return df.sort_values(["player_id", "date"])


def load_stats() -> pd.DataFrame:
    """
    Carga las estadísticas limpias por jornada.

    Returns
    -------
    pd.DataFrame
        DataFrame ordenado por jugador y jornada.
    """
    df = pd.read_csv(STATS_CLEAN_FILE)
    if "id" in df.columns:
        df.rename(columns={"id": "player_id"}, inplace=True)
    return df.sort_values(["player_id", "jornada"])


def load_calendar() -> pd.DataFrame:
    """
    Carga el calendario de partidos y normaliza fechas.

    Returns
    -------
    pd.DataFrame
        DataFrame ordenado cronológicamente.
    """
    df = pd.read_csv(CALENDAR_FILE)
    df["fecha_partido"] = pd.to_datetime(df["fecha_partido"], utc=True).dt.tz_localize(None)
    return df.sort_values("fecha_partido")


# =============================================================================
# FEATURE ENGINEERING
# =============================================================================

def enrich_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula variables acumuladas y rankings por jornada.

    Las variables se calculan de forma incremental para evitar
    data leakage: el valor de la jornada N solo incluye información
    hasta la jornada N-1 (cumsum desplazado).

    Parameters
    ----------
    df : pd.DataFrame
        Dataset de estadísticas limpias.

    Returns
    -------
    pd.DataFrame
        Dataset enriquecido con columnas adicionales de features.
    """
    df = df.sort_values(["player_id", "jornada"]).copy()
    grp = df.groupby("player_id")

    df["puntos_acumulados"]        = grp["puntos_totales"].cumsum()
    df["partidos_jugados"]         = grp.cumcount() + 1
    df["media_puntos_hasta_fecha"] = df["puntos_acumulados"] / df["partidos_jugados"]

    if "goals" in df.columns:
        df["goles_acumulados"] = grp["goals"].cumsum()

    if "posicion" in df.columns:
        df["ranking_posicion_jornada"] = df.groupby(
            ["jornada", "posicion"]
        )["puntos_totales"].rank(ascending=False, method="min")

    return df


def merge_market_stats(df_market: pd.DataFrame, df_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Cruza el historial diario de mercado con las estadísticas por jornada.

    Usa merge_asof con dirección 'backward' para asignar a cada snapshot
    de mercado las últimas estadísticas disponibles hasta esa fecha,
    evitando data leakage.

    Parameters
    ----------
    df_market : pd.DataFrame
        Historial diario de valor de mercado.
    df_stats : pd.DataFrame
        Estadísticas enriquecidas con columna 'fecha_jornada'.

    Returns
    -------
    pd.DataFrame
        Dataset combinado mercado + estadísticas.
    """
    return pd.merge_asof(
        df_market.sort_values("date"),
        df_stats.sort_values("fecha_jornada"),
        left_on="date",
        right_on="fecha_jornada",
        by="player_id",
        direction="backward",
    )


def add_calendar_features(df: pd.DataFrame, df_cal: pd.DataFrame) -> pd.DataFrame:
    """
    Añade variables temporales derivadas del calendario de partidos.

    Variables generadas
    -------------------
    dias_prox_partido    : días hasta el siguiente partido.
    prox_jornada         : número de la próxima jornada.
    is_paron_selecciones : True si hay parón internacional.
    dias_desde_inicio    : días desde el primer snapshot del dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Dataset maestro (mercado + stats).
    df_cal : pd.DataFrame
        Calendario con columnas 'fecha_partido' y 'jornada'.

    Returns
    -------
    pd.DataFrame
        Dataset con las nuevas columnas de calendario.
    """
    cal = df_cal[["fecha_partido", "jornada"]].rename(
        columns={"fecha_partido": "fecha_siguiente_partido", "jornada": "prox_jornada"}
    )

    df = pd.merge_asof(
        df.sort_values("date"),
        cal.sort_values("fecha_siguiente_partido"),
        left_on="date",
        right_on="fecha_siguiente_partido",
        direction="forward",
    )

    df["dias_prox_partido"]    = (df["fecha_siguiente_partido"] - df["date"]).dt.days
    df["is_paron_selecciones"] = df["dias_prox_partido"] > PARON_DIAS_UMBRAL
    df["dias_desde_inicio"]    = (df["date"] - df["date"].min()).dt.days

    return df


# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================

def build_master_dataset():
    """
    Ejecuta el pipeline completo de integración y feature engineering.

    Steps
    -----
    1. Carga los tres ficheros de data/raw/ y data/processed/.
    2. Enriquece las estadísticas con variables acumuladas.
    3. Asigna a cada jornada la fecha de su primer partido.
    4. Cruza el mercado diario con las stats via merge_asof backward.
    5. Añade las variables de calendario.
    6. Rellena NaN numéricos con 0 (pretemporada / datos faltantes).
    7. Guarda el dataset maestro en data/processed/.

    Returns
    -------
    pd.DataFrame
        Dataset maestro final.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("  📂 Cargando ficheros fuente...")
    df_market   = load_market()
    df_stats    = load_stats()
    df_calendar = load_calendar()

    print("  ⚙️  Calculando features acumuladas...")
    df_stats = enrich_stats(df_stats)

    # Fecha del primer partido de cada jornada
    fecha_jornadas = (
        df_calendar.groupby("jornada")["fecha_partido"]
        .min()
        .reset_index()
        .rename(columns={"fecha_partido": "fecha_jornada"})
    )
    df_stats = pd.merge(df_stats, fecha_jornadas, on="jornada", how="left")
    df_stats["fecha_jornada"] = pd.to_datetime(df_stats["fecha_jornada"])

    print("  🔄 Cruzando mercado con estadísticas (merge_asof)...")
    df_master = merge_market_stats(df_market, df_stats)

    print("  📅 Añadiendo variables de calendario...")
    df_master = add_calendar_features(df_master, df_calendar)

    # Rellenar NaN numéricos
    numeric_cols = df_master.select_dtypes(include=[np.number]).columns
    df_master[numeric_cols] = df_master[numeric_cols].fillna(0)

    df_master.to_csv(MASTER_DATASET_FILE, index=False)

    print(f"  ✅ Dataset Maestro: {MASTER_DATASET_FILE.name}"
          f"  ({df_master.shape[0]:,} filas × {df_master.shape[1]} columnas)")
    return df_master


if __name__ == "__main__":
    build_master_dataset()
