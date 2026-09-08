"""KOSPI/KOSDAQ 실지수 기반 재빌드 + 0150(98종목) 역산."""
from pathlib import Path

from condlab import features, api

print(features.build())

con = api._con()
D = "2026-09-07"
con.execute(f"CREATE OR REPLACE TABLE feat AS "
            f"SELECT * FROM read_parquet('{Path(features.config.FEAT_PQ).as_posix()}')")

print("\n=== 대형주 베타 (실지수 기준) ===")
print(con.execute(f"""
    SELECT code, name, market, beta60, beta120, beta250, beta360,
           CAST(amt20 / 100000000 AS BIGINT) AS amt20_억
    FROM feat WHERE d = DATE '{D}' AND sec_class = 'COMMON'
    ORDER BY amt20 DESC LIMIT 12
""").fetchdf().to_string())

print("\n=== 98종목 역산: 베타기간 × 거래대금기준 ===")
rows = []
for col in ("beta60", "beta120", "beta250", "beta360"):
    for label, amt_expr in (("20일평균", "amt20"), ("당일", "amt"), ("전일", "prev_amt")):
        for unit, cut in (("100억", 10_000_000_000), ("10억", 1_000_000_000)):
            n = con.execute(f"""
                SELECT count(*) FROM feat
                WHERE d = DATE '{D}' AND sec_class = 'COMMON'
                  AND {col} BETWEEN 1 AND 10 AND {amt_expr} >= {cut}
            """).fetchone()[0]
            rows.append((col, label, unit, n, abs(n - 98)))
for row in sorted(rows, key=lambda r: r[4])[:12]:
    print(f"{row[0]:<9} 거래대금 {row[1]:<7} {row[2]:<5} -> {row[3]:>5}종목  (오차 {row[4]})")
