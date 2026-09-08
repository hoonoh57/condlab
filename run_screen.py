from condlab import screens
def fmt(value):
    return f"{value:>9.2f}" if value is not None else f"{'-':>9}"


D0, D1 = "2026-06-01", "2026-09-07"

for name in ("base", "beta"):
    out = screens.verify(name, D0, D1)
    print(f"\n===== {out['label']} =====")
    print(f"포착 {out['n_hits']}건 / 하루 {out['per_day']}종목 / 모집단비중 {out['pct_of_pool']}%")
    print(f"{'':10}{'D+1':>9}{'D+3':>9}{'D+5':>9}{'D+10':>9}{'D+20':>9}")
    for label, key in (("조건식", "hit"), ("시장평균", "base")):
        row = out[key]
        print(f"{label:<10}" + "".join(fmt(row[f'r{m}']) for m in out["marks"]))
    print("초과수익  " + "".join(fmt(out['edge'][f'edge_r{m}']) for m in out["marks"]))
    print(f"승률 D+5 {out['hit']['win5']}% (시장 {out['base']['win5']}%) / "
          f"MFE20 {out['hit']['mfe']}% MAE20 {out['hit']['mae']}%")

top = screens.run("base", D0, D1, limit=15)
print("\n최근 기준봉돌파 15건")
for row in top["rows"]:
    print(f"{row['d']} {row['code']} {row['name'][:8]:<9} "
          f"{row['chg_pct']:>6.2f}% 기준봉{int(row['base_days'])}일전 "
          f"{row['base_chg']:>5.1f}% 고가대비{row['base_hi_over']:>6.2f}%")
