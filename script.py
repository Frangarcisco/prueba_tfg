import requests
import pandas as pd
import time
import random
from datetime import datetime

class LaLigaFantasyScraper:
    def __init__(self, token):
        self.base_url = "https://api-fantasy.llt-services.com"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": "Mozilla/5.0 (Linux; Android 9; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.74 Mobile Safari/537.36",
            "x-lang": "es",
            "Accept": "application/json"
        }

    def get_all_players(self):
        """Descarga la lista maestra de jugadores (ID, Nombre, Equipo)."""
        endpoint = "/api/v4/players?x-lang=es"
        url = f"{self.base_url}{endpoint}"
        try:
            print(f"📡 Descargando maestro de jugadores...")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data if isinstance(data, list) else data.get('players', []))
        except Exception as e:
            print(f"❌ Error maestro: {e}")
            return pd.DataFrame()

    def get_player_market_history(self, player_id, player_name):
        """Descarga el historial de precios (Timeseries)."""
        endpoint = f"/api/v3/player/{player_id}/market-value?x-lang=es"
        try:
            time.sleep(random.uniform(0.05, 0.1)) # Pequeña pausa
            response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                # Añadimos metadatos para cruzar tablas luego
                for d in data: 
                    d['player_id'] = player_id
                    d['player_name'] = player_name
                return data
            return []
        except:
            return []

    def get_player_full_stats(self, player_id):
        """
        NUEVO: Descarga TODAS las estadísticas del jugador.
        Endpoint: /api/v3/player/{id}
        """
        endpoint = f"/api/v3/player/{player_id}?x-lang=es"
        try:
            # Pausa de seguridad para no saturar
            time.sleep(random.uniform(0.05, 0.1)) 
            
            response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                # Aquí está el truco: Devolvemos el JSON entero.
                # Pandas se encargará de aplanarlo y sacar todas las columnas posibles.
                return data
            else:
                print(f"   ⚠️ Sin stats para ID {player_id} (Code {response.status_code})")
                return None
        except Exception as e:
            print(f"   ❌ Error stats ID {player_id}: {e}")
            return None

# --- CONFIGURACIÓN ---
MY_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkNBdXdPcWRMN2YyXzlhTVhZX3ZkbEcyVENXbVV4aklXV1MwNVB4WHljcUkiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJjZjExMDgyNy1lNGE5LTRkMjAtYWZmYi04ZWEwYzZmMTVmOTQiLCJpc3MiOiJodHRwczovL2xvZ2luLmxhbGlnYS5lcy8zMzUzMTZlYi1mNjA2LTQzNjEtYmI4Ni0zNWE3ZWRjZGNlYzEvdjIuMC8iLCJleHAiOjE3NzAxMzYxMDcsIm5iZiI6MTc3MDA0OTcwNywiaWRwIjoiZ29vZ2xlLmNvbSIsImlzQWN0dWFsVmVyc2lvbiI6dHJ1ZSwiY29ycmVsYXRpb25JZCI6ImQ1ODFmYmFkLWEzNzQtNDUwNS05MzgyLWI1OTQzOWVkZmVhZSIsInRpZCI6IjMzNTMxNmViLWY2MDYtNDM2MS1iYjg2LTM1YTdlZGNkY2VjMSIsImxhIjoiZXMtZXMiLCJlbWFpbCI6ImZyYW5jaXNjb2xvc2FsZWpvc0BnbWFpbC5jb20iLCJnaXZlbl9uYW1lIjoiRnJhbmNpc2NvIiwiZmFtaWx5X25hbWUiOiJHYXJjaWEiLCJuYW1lIjoiR29vZ2xlIHVzZXIiLCJzdWIiOiJmYmNkYjBjNy0zODhmLTRiM2QtYTU0NS01NjdlNWY1ODU2YzciLCJvdGhlck1haWxzIjpbImZyYW5jaXNjb2xvc2FsZWpvc0BnbWFpbC5jb20iXSwiZXh0ZW5zaW9uX1VzZXJQcm9maWxlSWQiOiIzODI3OTY2ZS0yNWVhLTQ4ZGEtYmI1ZS1jZjA4MTlmYzU0N2EiLCJvaWQiOiIzODI3OTY2ZS0yNWVhLTQ4ZGEtYmI1ZS1jZjA4MTlmYzU0N2EiLCJleHRlbnNpb25fRW1haWxWZXJpZmllZCI6ZmFsc2UsIm5ld1VzZXIiOmZhbHNlLCJoYXNQZW5kaW5nQ29uc2VudHMiOmZhbHNlLCJub25jZSI6IkZleTZGQUZfd3dFNXprSXMzd1hmMXciLCJhenAiOiJjZjExMDgyNy1lNGE5LTRkMjAtYWZmYi04ZWEwYzZmMTVmOTQiLCJ2ZXIiOiIxLjAiLCJpYXQiOjE3NzAwNDk3MDd9.g9G2YUCSbmbgd-vABH4PgwvhtA5tlFeShjsW3AA9_2NBM4QXvS8Qoh9TUb4OYhgo71oApFI3ecs_4TPbd0KYfhRJ4AMkas0A1M2yse2SBVUe1X44eU-DxaoTPRhgk-hBsI3d46USBeSQlce2NHl1Fl3a-cNN0Oq7lvJQHSmlKmHcbxQeoot05ZTUllXqvn4UhFCfvzsjBqoFowGpWjyQtq1E-e-advdmAdFd7h-Z4rEebcYkpfSlh0E6N9FtUJhTLrHZ5t08EXT9HshKmXdltvQY9wug0XL0nFuGi5ZEiFUIMx_8HjLhKdrBcgUjXxrvR2NhbBoR8y8KM_xTSUNtfA" 


# --- EJECUCIÓN ---
if __name__ == "__main__":
    scraper = LaLigaFantasyScraper(MY_TOKEN)

    # 1. Maestro de Jugadores
    df_players = scraper.get_all_players()
    
    if not df_players.empty:
        print(f"✅ Jugadores encontrados: {len(df_players)}")
        df_players.to_csv("maestro_jugadores.csv", index=False)

        # Listas para acumular los datos
        market_data_accumulator = []
        stats_data_accumulator = []

        print(f"\n🚀 Iniciando extracción masiva (Mercado + Stats)...")
        print("☕ Paciencia: procesando ~0.5 segundos por jugador...")

        total = len(df_players)
        
        # Bucle principal: Recorre cada jugador UNA sola vez
        for index, row in df_players.iterrows():
            p_id = row['id']
            p_name = row.get('nickname', 'Unknown')
            
            if index % 5 == 0:
                print(f"   [{index+1}/{total}] Procesando: {p_name}...")

            # A) Obtener Mercado
            market = scraper.get_player_market_history(p_id, p_name)
            market_data_accumulator.extend(market)

            # B) Obtener Estadísticas (NUEVO)
            stats = scraper.get_player_full_stats(p_id)
            if stats:
                stats_data_accumulator.append(stats)

        # ---------------- GUARDADO DE DATOS ----------------
        
        # 1. Guardar CSV de Mercado (Series Temporales)
        if market_data_accumulator:
            df_market = pd.DataFrame(market_data_accumulator)
            if 'date' in df_market.columns:
                df_market['date'] = pd.to_datetime(df_market['date'], utc=True)
            
            filename_market = f"dataset_mercado_{datetime.now().strftime('%Y%m%d')}.csv"
            df_market.to_csv(filename_market, index=False)
            print(f"\n📈 [Mercado] Guardado: {filename_market} ({len(df_market)} filas)")

        # 2. Guardar CSV de Estadísticas (Perfil Completo)
        if stats_data_accumulator:
            # json_normalize es MAGIA: convierte JSONs anidados en columnas planas
            # Ej: stats['goals'] se convierte en columna 'stats.goals'
            df_stats = pd.json_normalize(stats_data_accumulator)
            
            filename_stats = f"dataset_stats_jugadores_{datetime.now().strftime('%Y%m%d')}.csv"
            df_stats.to_csv(filename_stats, index=False)
            print(f"📊 [Stats] Guardado: {filename_stats} ({len(df_stats)} jugadores)")
            
            # Preview de columnas interesantes que podrías encontrar
            print("\n🔍 Ejemplo de columnas extraídas en Stats:")
            interesting_cols = [c for c in df_stats.columns if 'points' in c or 'goal' in c or 'match' in c]
            print(interesting_cols[:10]) # Muestra las primeras 10 que parezcan stats
            
    else:
        print("❌ Falló la descarga inicial de jugadores. Revisa el Token.")

