import os
import pandas as pd
from datetime import datetime
from market import get_market, parse_player

DATA_DIR = "data/market"

def save_daily_market():
    os.makedirs(DATA_DIR, exist_ok=True)

    players = get_market()
    parsed = [p for p in (parse_player(p) for p in players) if p]

    df = pd.DataFrame(parsed)
    df["date"] = datetime.now().date()

    filename = os.path.join(DATA_DIR, f"{df['date'][0]}.csv")
    df.to_csv(filename, index=False)

    print(f"✅ Mercado guardado: {filename}")

if __name__ == "__main__":
    save_daily_market()
