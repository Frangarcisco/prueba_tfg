import os
import pandas as pd

MARKET_DIR = "data/market"
POINTS_DIR = "data/points"
OUTPUT_FILE = "data/dataset.csv"

def build_dataset():
    market_files = sorted(os.listdir(MARKET_DIR))
    points_files = sorted(os.listdir(POINTS_DIR))

    market_dfs = []
    points_dfs = []

    # --- MARKET ---
    for f in market_files:
        if f.endswith(".csv"):
            df = pd.read_csv(os.path.join(MARKET_DIR, f))
            market_dfs.append(df)

    market_df = pd.concat(market_dfs, ignore_index=True)

    # --- POINTS ---
    for f in points_files:
        if f.endswith(".csv"):
            df = pd.read_csv(os.path.join(POINTS_DIR, f))
            points_dfs.append(df)

    points_df = pd.concat(points_dfs, ignore_index=True)

    # --- LIMPIEZA ---
    points_df = points_df[[
        "id", "points", "jornada", "season", "date"
    ]]

    # --- MERGE ---
    dataset = market_df.merge(
        points_df,
        on=["id", "date"],
        how="left"
    )

    dataset.sort_values(["date", "id"], inplace=True)

    os.makedirs("data", exist_ok=True)
    dataset.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Dataset actualizado: {OUTPUT_FILE}")

if __name__ == "__main__":
    build_dataset()
