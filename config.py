"""
config.py
=========
Configuración centralizada del proyecto TFG.

Todas las constantes, rutas, credenciales y parámetros globales
se definen aquí para facilitar el mantenimiento y evitar valores
hardcodeados dispersos por el código.

IMPORTANTE: En un entorno de producción el token JWT debería cargarse
desde una variable de entorno o fichero .env, nunca commitearse en el
repositorio. Para desarrollo local, puedes pegarlo directamente en
LALIGA_TOKEN_DEFAULT.
"""

import os
from pathlib import Path

# =============================================================================
# RUTAS DEL PROYECTO
# =============================================================================

BASE_DIR      = Path(__file__).parent
DATA_DIR      = BASE_DIR / "data"
RAW_DIR       = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Ficheros de salida en raw/
MARKET_HISTORY_FILE = RAW_DIR / "dataset_mercado.csv"
STATS_RAW_FILE      = RAW_DIR / "dataset_stats_jugadores.csv"
CALENDAR_FILE       = RAW_DIR / "calendario_laliga.csv"

# Ficheros de salida en processed/
STATS_CLEAN_FILE    = PROCESSED_DIR / "dataset_entrenamiento_FINAL.csv"
MASTER_DATASET_FILE = PROCESSED_DIR / "DATASET_MAESTRO_TFG.csv"

# =============================================================================
# CREDENCIALES
# =============================================================================

# Carga desde variable de entorno; si no existe, usa el valor por defecto.
# En terminal:  export LALIGA_TOKEN="tu_token_aqui"   (Linux/Mac)
#               $env:LALIGA_TOKEN="tu_token_aqui"     (PowerShell)
LALIGA_TOKEN_DEFAULT = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkNBdXdPcWRMN2YyXzlhTVhZX3ZkbEcyVENXbVV4aklXV1MwNVB4WHljcUkiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJjZjExMDgyNy1lNGE5LTRkMjAtYWZmYi04ZWEwYzZmMTVmOTQiLCJpc3MiOiJodHRwczovL2xvZ2luLmxhbGlnYS5lcy8zMzUzMTZlYi1mNjA2LTQzNjEtYmI4Ni0zNWE3ZWRjZGNlYzEvdjIuMC8iLCJleHAiOjE3NzAxMzYxMDcsIm5iZiI6MTc3MDA0OTcwNywiaWRwIjoiZ29vZ2xlLmNvbSIsImlzQWN0dWFsVmVyc2lvbiI6dHJ1ZSwiY29ycmVsYXRpb25JZCI6ImQ1ODFmYmFkLWEzNzQtNDUwNS05MzgyLWI1OTQzOWVkZmVhZSIsInRpZCI6IjMzNTMxNmViLWY2MDYtNDM2MS1iYjg2LTM1YTdlZGNkY2VjMSIsImxhIjoiZXMtZXMiLCJlbWFpbCI6ImZyYW5jaXNjb2xvc2FsZWpvc0BnbWFpbC5jb20iLCJnaXZlbl9uYW1lIjoiRnJhbmNpc2NvIiwiZmFtaWx5X25hbWUiOiJHYXJjaWEiLCJuYW1lIjoiR29vZ2xlIHVzZXIiLCJzdWIiOiJmYmNkYjBjNy0zODhmLTRiM2QtYTU0NS01NjdlNWY1ODU2YzciLCJvdGhlck1haWxzIjpbImZyYW5jaXNjb2xvc2FsZWpvc0BnbWFpbC5jb20iXSwiZXh0ZW5zaW9uX1VzZXJQcm9maWxlSWQiOiIzODI3OTY2ZS0yNWVhLTQ4ZGEtYmI1ZS1jZjA4MTlmYzU0N2EiLCJvaWQiOiIzODI3OTY2ZS0yNWVhLTQ4ZGEtYmI1ZS1jZjA4MTlmYzU0N2EiLCJleHRlbnNpb25fRW1haWxWZXJpZmllZCI6ZmFsc2UsIm5ld1VzZXIiOmZhbHNlLCJoYXNQZW5kaW5nQ29uc2VudHMiOmZhbHNlLCJub25jZSI6IkZleTZGQUZfd3dFNXprSXMzd1hmMXciLCJhenAiOiJjZjExMDgyNy1lNGE5LTRkMjAtYWZmYi04ZWEwYzZmMTVmOTQiLCJ2ZXIiOiIxLjAiLCJpYXQiOjE3NzAwNDk3MDd9.g9G2YUCSbmbgd-vABH4PgwvhtA5tlFeShjsW3AA9_2NBM4QXvS8Qoh9TUb4OYhgo71oApFI3ecs_4TPbd0KYfhRJ4AMkas0A1M2yse2SBVUe1X44eU-DxaoTPRhgk-hBsI3d46USBeSQlce2NHl1Fl3a-cNN0Oq7lvJQHSmlKmHcbxQeoot05ZTUllXqvn4UhFCfvzsjBqoFowGpWjyQtq1E-e-advdmAdFd7h-Z4rEebcYkpfSlh0E6N9FtUJhTLrHZ5t08EXT9HshKmXdltvQY9wug0XL0nFuGi5ZEiFUIMx_8HjLhKdrBcgUjXxrvR2NhbBoR8y8KM_xTSUNtfA" 
LALIGA_TOKEN = os.getenv("LALIGA_TOKEN", LALIGA_TOKEN_DEFAULT)

# =============================================================================
# URLs DE LA API
# =============================================================================

LALIGA_BASE_URL = "https://api-fantasy.llt-services.com"

# =============================================================================
# PARÁMETROS DE LA TEMPORADA
# =============================================================================

SEASON         = 2025
TOTAL_JORNADAS = 38

# Umbral en días para detectar parón de selecciones
PARON_DIAS_UMBRAL = 12

# =============================================================================
# PARÁMETROS DE SCRAPING
# =============================================================================

# Pausa aleatoria entre peticiones para no saturar la API
REQUEST_DELAY_MIN = 0.05   # segundos
REQUEST_DELAY_MAX = 0.10   # segundos

HEADERS_LALIGA = {
    "Authorization": f"Bearer {LALIGA_TOKEN}",
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 9; SM-G973F) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/95.0.4638.74 Mobile Safari/537.36"
    ),
    "x-lang": "es",
    "Accept": "application/json",
}
