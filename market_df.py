import pandas as pd
from market import get_market, parse_player

players = get_market()
parsed = [p for p in (parse_player(p) for p in players) if p]

df = pd.DataFrame(parsed)

print(df.head())
print(df.info())

# Top 10 jugadores que más suben
top_up = df.sort_values("market_variation", ascending=False).head(10)
print("\nTOP SUBIDAS")
print(top_up[["name", "market_value", "market_variation"]])

# Top 10 que más bajan
top_down = df.sort_values("market_variation").head(10)
print("\nTOP BAJADAS")
print(top_down[["name", "market_value", "market_variation"]])
