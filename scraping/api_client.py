"""
scraping/api_client.py
======================
Cliente HTTP para la API oficial de LaLiga Fantasy.

Centraliza la autenticación JWT, la gestión de errores y las pausas
entre peticiones. Todos los módulos de scraping usan esta clase en
lugar de hacer llamadas directas con requests.
"""

import time
import random
import requests
from typing import Any

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    LALIGA_BASE_URL,
    HEADERS_LALIGA,
    REQUEST_DELAY_MIN,
    REQUEST_DELAY_MAX,
)


class LaLigaAPIClient:
    """
    Cliente para la API oficial de LaLiga Fantasy.

    Gestiona la autenticación Bearer y añade una pausa aleatoria entre
    peticiones para respetar los límites de la API.

    Parameters
    ----------
    delay_min : float
        Tiempo mínimo de espera (segundos) entre peticiones.
    delay_max : float
        Tiempo máximo de espera (segundos) entre peticiones.
    """

    def __init__(
        self,
        delay_min: float = REQUEST_DELAY_MIN,
        delay_max: float = REQUEST_DELAY_MAX,
    ):
        self.base_url  = LALIGA_BASE_URL
        self.headers   = HEADERS_LALIGA
        self.delay_min = delay_min
        self.delay_max = delay_max

    def _get(self, endpoint: str) -> Any:
        """
        Realiza una petición GET autenticada.

        Parameters
        ----------
        endpoint : str
            Ruta relativa del endpoint (p.ej. '/api/v4/players').

        Returns
        -------
        Any
            JSON de la respuesta parseado, o None si hay error.
        """
        time.sleep(random.uniform(self.delay_min, self.delay_max))
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            print(f"  ⚠️  HTTP {response.status_code} en {endpoint}")
        except requests.RequestException as e:
            print(f"  ❌ Error de red en {endpoint}: {e}")
        return None

    # ------------------------------------------------------------------
    # Endpoints de jugadores
    # ------------------------------------------------------------------

    def get_all_players(self) -> list:
        """
        Descarga el listado maestro de jugadores de la temporada.

        Returns
        -------
        list[dict]
            Lista de jugadores con id, nombre, equipo y posición.
        """
        data = self._get("/api/v4/players?x-lang=es")
        if data is None:
            return []
        return data if isinstance(data, list) else data.get("players", [])

    def get_player_market_history(self, player_id: int) -> list:
        """
        Descarga el historial completo de valor de mercado de un jugador.

        Parameters
        ----------
        player_id : int
            Identificador único del jugador.

        Returns
        -------
        list[dict]
            Lista de registros {date, marketValue, player_id}.
        """
        data = self._get(f"/api/v3/player/{player_id}/market-value?x-lang=es")
        if not data:
            return []
        for record in data:
            record["player_id"] = player_id
        return data

    def get_player_stats(self, player_id: int) -> dict | None:
        """
        Descarga el perfil completo con estadísticas de un jugador.

        Devuelve el JSON completo de la API, que incluye estadísticas
        por jornada (goles, asistencias, minutos, puntos, etc.).

        Parameters
        ----------
        player_id : int
            Identificador único del jugador.

        Returns
        -------
        dict or None
            Perfil completo del jugador, o None si hay error.
        """
        return self._get(f"/api/v3/player/{player_id}?x-lang=es")

    # ------------------------------------------------------------------
    # Endpoint de calendario
    # ------------------------------------------------------------------

    def get_calendar_week(self, week_number: int) -> list:
        """
        Descarga los partidos de una jornada específica.

        Parameters
        ----------
        week_number : int
            Número de jornada (1-38).

        Returns
        -------
        list[dict]
            Lista de partidos con equipos locales, visitantes y resultado.
        """
        data = self._get(f"/api/v3/calendar?weekNumber={week_number}&x-lang=es")
        if data is None:
            return []
        return data if isinstance(data, list) else []
