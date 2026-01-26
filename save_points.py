import os
import pandas as pd
from datetime import datetime
from points import get_points, parse_points

DATA_DIR = "data/points"

def save_daily_points():
    os.makedirs(DATA_DIR, exist_ok=True)

    jornada, season, players = get_points()
    parsed = [parse_points(p) for p in players]

    df = pd.DataFrame(parsed)
    df["jornada"] = jornada
    df["season"] = season
    df["date"] = datetime.now().date()

    filename = os.path.join(
        DATA_DIR,
        f"{season}_J{jornada}_{df['date'][0]}.csv"
    )

    df.to_csv(filename, index=False)
    print(f"✅ Puntos guardados: {filename}")

if __name__ == "__main__":
    save_daily_points()
