import requests

URL = "https://app.analiticafantasy.com/api/puntos-fantasy"

def get_points():
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    payload = {
        "season": 2025,
        "jornada": -1,   # 👈 CLAVE
        "fantasy": 2
    }

    r = requests.post(URL, headers=headers, json=payload)
    r.raise_for_status()
    data = r.json()

    players = data["players"]
    jornada = data.get("jornadaActiva", -1)
    season = payload["season"]

    return jornada, season, players


def parse_points(raw):
    return {
        "id": raw["id"],
        "name": raw["name"],
        "team_id": raw["t"],
        "points": int(raw["p"]),
        "position": raw["pos"],
        "market_value": raw["vm"],
        "variation": raw["su"]
    }
