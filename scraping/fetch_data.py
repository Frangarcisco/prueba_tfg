"""
scraping/fetch_data.py
======================
Extracción completa de datos de la temporada desde la API de LaLiga Fantasy.

Este es el único script de scraping necesario. Realiza en una sola ejecución:
  1. Descarga el listado maestro de todos los jugadores de la temporada.
  2. Por cada jugador, descarga su historial de valor de mercado (series
     temporales diarias desde el inicio de temporada hasta hoy).
  3. Por cada jugador, descarga sus estadísticas completas por jornada
     (goles, asistencias, minutos, puntos fantasy, tarjetas, etc.).
  4. Descarga el calendario completo de las 38 jornadas.
  5. Guarda los tres CSV resultantes en data/raw/.

Los ficheros generados son la entrada directa del pipeline de procesamiento
(data_processing/).

Uso
---
    python main.py          ← recomendado (desde la raíz)
    python -m scraping.fetch_data
"""

import sys
from pathlib import Path

# Garantiza que la raíz del proyecto esté en el path de Python,
# independientemente del directorio desde el que se ejecute el script.
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from datetime import datetime

from config import (
    RAW_DIR,
    MARKET_HISTORY_FILE,
    STATS_RAW_FILE,
    CALENDAR_FILE,
    TOTAL_JORNADAS,
)
from scraping.api_client import LaLigaAPIClient


# =============================================================================
# PARSERS
# =============================================================================

def parse_market_record(record: dict, player_name: str) -> dict:
    """
    Normaliza un registro del historial de mercado.

    Parameters
    ----------
    record : dict
        Registro crudo devuelto por la API de historial de mercado.
    player_name : str
        Nombre del jugador, para enriquecer el registro.

    Returns
    -------
    dict
        Registro normalizado con player_id, player_name, date y marketValue.
    """
    return {
        "player_id":   record.get("player_id"),
        "player_name": player_name,
        "date":        record.get("date"),
        "marketValue": record.get("marketValue") or record.get("value"),
    }


def parse_calendar_match(raw: dict, jornada: int) -> dict:
    """
    Normaliza los campos relevantes de un partido del calendario.

    Parameters
    ----------
    raw : dict
        Objeto partido devuelto por la API de calendario.
    jornada : int
        Número de jornada al que pertenece el partido.

    Returns
    -------
    dict
        Diccionario con fecha, equipos, goles y estado del partido.
    """
    return {
        "jornada":          jornada,
        "fecha_partido":    raw.get("date"),
        "local_id":         raw.get("local",   {}).get("id"),
        "local_nombre":     raw.get("local",   {}).get("name"),
        "local_goles":      raw.get("localScore"),
        "visitante_id":     raw.get("visitor", {}).get("id"),
        "visitante_nombre": raw.get("visitor", {}).get("name"),
        "visitante_goles":  raw.get("visitorScore"),
        "estado":           raw.get("status"),
    }


# =============================================================================
# FUNCIONES DE EXTRACCIÓN
# =============================================================================

def fetch_players_and_stats(client: LaLigaAPIClient) -> tuple[list, list]:
    """
    Descarga el historial de mercado y las estadísticas de todos los jugadores.

    Realiza una única pasada por el listado maestro de jugadores y,
    para cada uno, lanza dos peticiones a la API: historial de mercado
    y perfil con estadísticas por jornada.

    Parameters
    ----------
    client : LaLigaAPIClient
        Instancia del cliente autenticado.

    Returns
    -------
    tuple[list, list]
        - market_records : lista de registros de mercado (uno por día/jugador).
        - stats_records  : lista de perfiles completos (uno por jugador).
    """
    print("📡 Descargando listado maestro de jugadores...")
    players = client.get_all_players()

    if not players:
        print("❌ No se pudo obtener el listado de jugadores. Revisa el token en config.py.")
        return [], []

    total = len(players)
    print(f"✅ {total} jugadores encontrados.\n")
    print(f"🚀 Descargando mercado + estadísticas ({total} jugadores)...")
    print("   (Esto puede tardar varios minutos)\n")

    market_records = []
    stats_records  = []

    for i, player in enumerate(players):
        player_id   = player["id"]
        player_name = player.get("nickname", "Unknown")

        # Progreso cada 25 jugadores
        if i % 25 == 0:
            pct = round((i / total) * 100)
            print(f"   [{i + 1:>4}/{total}]  {pct:>3}%  —  {player_name}")

        # Historial de mercado
        for record in client.get_player_market_history(player_id):
            market_records.append(parse_market_record(record, player_name))

        # Estadísticas por jornada
        stats = client.get_player_stats(player_id)
        if stats:
            stats_records.append(stats)

    print(f"\n✅ Extracción completada: {len(market_records)} registros de mercado, "
          f"{len(stats_records)} perfiles de estadísticas.")
    return market_records, stats_records


def fetch_calendar(client: LaLigaAPIClient) -> list:
    """
    Descarga el calendario completo de la temporada (38 jornadas).

    Parameters
    ----------
    client : LaLigaAPIClient
        Instancia del cliente autenticado.

    Returns
    -------
    list[dict]
        Lista de partidos normalizados de todas las jornadas.
    """
    # Usamos una pausa un poco mayor para el calendario, ya que
    # son 38 peticiones seguidas al mismo endpoint.
    cal_client = LaLigaAPIClient(delay_min=0.2, delay_max=0.4)

    print(f"📅 Descargando calendario ({TOTAL_JORNADAS} jornadas)...")
    all_matches = []

    for jornada in range(1, TOTAL_JORNADAS + 1):
        print(f"   Jornada {jornada:02d}/{TOTAL_JORNADAS}...", end="\r")
        matches = cal_client.get_calendar_week(jornada)
        for match in matches:
            all_matches.append(parse_calendar_match(match, jornada))

    print(f"✅ Calendario descargado: {len(all_matches)} partidos.         ")
    return all_matches


# =============================================================================
# PERSISTENCIA
# =============================================================================

def save_market(market_records: list):
    """
    Guarda el historial de mercado en data/raw/dataset_mercado.csv.

    Parameters
    ----------
    market_records : list[dict]
        Lista de registros de mercado devuelta por fetch_players_and_stats.
    """
    if not market_records:
        print("⚠️  Sin datos de mercado, no se genera el fichero.")
        return

    df = pd.DataFrame(market_records)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df.sort_values(["player_id", "date"], inplace=True)

    df.to_csv(MARKET_HISTORY_FILE, index=False)
    print(f"💾 Mercado guardado:      {MARKET_HISTORY_FILE}  ({len(df):,} filas)")


def save_stats(stats_records: list):
    """
    Guarda las estadísticas de jugadores en data/raw/dataset_stats_jugadores.csv.

    Usa pd.json_normalize para aplanar el JSON anidado de la API en columnas
    individuales (p.ej. playerStats, marketValue, etc.).

    Parameters
    ----------
    stats_records : list[dict]
        Lista de perfiles de jugador devuelta por fetch_players_and_stats.
    """
    if not stats_records:
        print("⚠️  Sin datos de estadísticas, no se genera el fichero.")
        return

    df = pd.json_normalize(stats_records)
    df.to_csv(STATS_RAW_FILE, index=False)
    print(f"💾 Estadísticas guardadas: {STATS_RAW_FILE}  ({len(df):,} jugadores)")


def save_calendar(match_records: list):
    """
    Guarda el calendario en data/raw/calendario_laliga.csv.

    Parameters
    ----------
    match_records : list[dict]
        Lista de partidos devuelta por fetch_calendar.
    """
    if not match_records:
        print("⚠️  Sin datos de calendario, no se genera el fichero.")
        return

    df = pd.DataFrame(match_records)
    df["fecha_partido"] = pd.to_datetime(df["fecha_partido"], utc=True)
    df.sort_values(["jornada", "fecha_partido"], inplace=True)

    df.to_csv(CALENDAR_FILE, index=False)
    print(f"💾 Calendario guardado:   {CALENDAR_FILE}  ({len(df):,} partidos)")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

def fetch_all():
    """
    Ejecuta la extracción completa de datos de la temporada.

    Orquesta en orden:
      1. Mercado histórico + estadísticas de jugadores.
      2. Calendario de jornadas.
      3. Persistencia de los tres ficheros CSV en data/raw/.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    start = datetime.now()
    print("=" * 60)
    print("  EXTRACCIÓN DE DATOS — LaLiga Fantasy API")
    print(f"  Inicio: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")

    client = LaLigaAPIClient()

    # 1. Jugadores: mercado + stats
    market_records, stats_records = fetch_players_and_stats(client)

    # 2. Calendario
    print()
    match_records = fetch_calendar(client)

    # 3. Guardado
    print("\n📂 Guardando ficheros en data/raw/ ...")
    save_market(market_records)
    save_stats(stats_records)
    save_calendar(match_records)

    elapsed = (datetime.now() - start).seconds
    print(f"\n✅ Todo listo en {elapsed}s. Ficheros guardados en data/raw/")
    print("   Siguiente paso: ejecutar el pipeline de procesamiento.")
    print("   →  python main.py --process\n")


if __name__ == "__main__":
    fetch_all()
