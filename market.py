import requests

URL = "https://app.analiticafantasy.com/api/fantasy-players/mercado"

def get_market():
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    r = requests.post(URL, headers=headers, json={})
    r.raise_for_status()
    return r.json()["players"]

def parse_player(raw):
    info = raw.get("i")
    mv = raw.get("mv")

    if not isinstance(info, dict) or not isinstance(mv, dict):
        return None

    if "i" not in info or "n" not in info or "p" not in info:
        return None

    return {
        "id": info["i"],
        "name": info["n"],
        "position": info["p"],
        "team_id": raw.get("t"),
        "market_value": mv.get("v"),
        "market_variation": mv.get("d")
    }


if __name__ == "__main__":
    players = get_market()
    parsed = [p for p in (parse_player(p) for p in players) if p]
    print(parsed[:3])

