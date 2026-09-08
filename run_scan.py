from condlab.api import scan

r = scan("2026-09-04")
print("hits:", r["n_hits"], "elapsed:", r["elapsed"])
for h in r["hits"]:
    print(
        h["code"],
        h["name"],
        h["market"],
        h["close_px"],
        h["breakout_px"],
        h["chg_pct"],
        h["vol_ratio"],
        h["over_pct"],
    )