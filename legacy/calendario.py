import requests
import pandas as pd
import time
import random
from datetime import datetime

class LaLigaCalendarScraper:
    def __init__(self, token):
        self.base_url = "https://api-fantasy.llt-services.com"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": "Mozilla/5.0 (Linux; Android 9; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.74 Mobile Safari/537.36",
            "x-lang": "es",
            "Accept": "application/json"
        }

    def get_matches_for_week(self, week_num):
        """Descarga los partidos de una jornada específica."""
        endpoint = f"/api/v3/calendar?weekNumber={week_num}&x-lang=es"
        url = f"{self.base_url}{endpoint}"
        
        try:
            # Pausa para no saturar
            time.sleep(random.uniform(0.2, 0.5))
            
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"⚠️ Error en Jornada {week_num}: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Excepción en Jornada {week_num}: {e}")
            return []

# --- CONFIGURACIÓN ---
MY_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkNBdXdPcWRMN2YyXzlhTVhZX3ZkbEcyVENXbVV4aklXV1MwNVB4WHljcUkiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJjZjExMDgyNy1lNGE5LTRkMjAtYWZmYi04ZWEwYzZmMTVmOTQiLCJpc3MiOiJodHRwczovL2xvZ2luLmxhbGlnYS5lcy8zMzUzMTZlYi1mNjA2LTQzNjEtYmI4Ni0zNWE3ZWRjZGNlYzEvdjIuMC8iLCJleHAiOjE3NzAxMzYxMDcsIm5iZiI6MTc3MDA0OTcwNywiaWRwIjoiZ29vZ2xlLmNvbSIsImlzQWN0dWFsVmVyc2lvbiI6dHJ1ZSwiY29ycmVsYXRpb25JZCI6ImQ1ODFmYmFkLWEzNzQtNDUwNS05MzgyLWI1OTQzOWVkZmVhZSIsInRpZCI6IjMzNTMxNmViLWY2MDYtNDM2MS1iYjg2LTM1YTdlZGNkY2VjMSIsImxhIjoiZXMtZXMiLCJlbWFpbCI6ImZyYW5jaXNjb2xvc2FsZWpvc0BnbWFpbC5jb20iLCJnaXZlbl9uYW1lIjoiRnJhbmNpc2NvIiwiZmFtaWx5X25hbWUiOiJHYXJjaWEiLCJuYW1lIjoiR29vZ2xlIHVzZXIiLCJzdWIiOiJmYmNkYjBjNy0zODhmLTRiM2QtYTU0NS01NjdlNWY1ODU2YzciLCJvdGhlck1haWxzIjpbImZyYW5jaXNjb2xvc2FsZWpvc0BnbWFpbC5jb20iXSwiZXh0ZW5zaW9uX1VzZXJQcm9maWxlSWQiOiIzODI3OTY2ZS0yNWVhLTQ4ZGEtYmI1ZS1jZjA4MTlmYzU0N2EiLCJvaWQiOiIzODI3OTY2ZS0yNWVhLTQ4ZGEtYmI1ZS1jZjA4MTlmYzU0N2EiLCJleHRlbnNpb25fRW1haWxWZXJpZmllZCI6ZmFsc2UsIm5ld1VzZXIiOmZhbHNlLCJoYXNQZW5kaW5nQ29uc2VudHMiOmZhbHNlLCJub25jZSI6IkZleTZGQUZfd3dFNXprSXMzd1hmMXciLCJhenAiOiJjZjExMDgyNy1lNGE5LTRkMjAtYWZmYi04ZWEwYzZmMTVmOTQiLCJ2ZXIiOiIxLjAiLCJpYXQiOjE3NzAwNDk3MDd9.g9G2YUCSbmbgd-vABH4PgwvhtA5tlFeShjsW3AA9_2NBM4QXvS8Qoh9TUb4OYhgo71oApFI3ecs_4TPbd0KYfhRJ4AMkas0A1M2yse2SBVUe1X44eU-DxaoTPRhgk-hBsI3d46USBeSQlce2NHl1Fl3a-cNN0Oq7lvJQHSmlKmHcbxQeoot05ZTUllXqvn4UhFCfvzsjBqoFowGpWjyQtq1E-e-advdmAdFd7h-Z4rEebcYkpfSlh0E6N9FtUJhTLrHZ5t08EXT9HshKmXdltvQY9wug0XL0nFuGi5ZEiFUIMx_8HjLhKdrBcgUjXxrvR2NhbBoR8y8KM_xTSUNtfA" 

# --- EJECUCIÓN ---
if __name__ == "__main__":
    scraper = LaLigaCalendarScraper(MY_TOKEN)
    
    all_matches = []
    
    print("📅 Descargando Calendario de LaLiga (Jornadas 1 a 38)...")
    
    for i in range(1, 39):
        print(f"   Descargando Jornada {i}...")
        matches = scraper.get_matches_for_week(i)
        
        # Procesamos la respuesta para sacar lo útil
        for match in matches:
            # A veces la estructura cambia, intentamos normalizar
            match_info = {
                'jornada': i,
                'fecha_partido': match.get('date'), # Fecha y hora
                'local_id': match.get('local', {}).get('id'),
                'local_nombre': match.get('local', {}).get('name'),
                'local_goles': match.get('localScore'),
                'visitante_id': match.get('visitor', {}).get('id'),
                'visitante_nombre': match.get('visitor', {}).get('name'),
                'visitante_goles': match.get('visitorScore'),
                'estado': match.get('status') # Si está jugado, aplazado, etc.
            }
            all_matches.append(match_info)

    # Guardar
    if all_matches:
        df_calendar = pd.DataFrame(all_matches)
        
        # Limpieza de fechas
        if 'fecha_partido' in df_calendar.columns:
            df_calendar['fecha_partido'] = pd.to_datetime(df_calendar['fecha_partido'])
            
        filename = f"calendario_laliga_2025.csv"
        df_calendar.to_csv(filename, index=False)
        print(f"\n✅ ¡HECHO! Calendario guardado en: {filename}")
        print(df_calendar.head())
    else:
        print("❌ No se pudieron descargar partidos.")