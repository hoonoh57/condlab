import duckdb, time

from condlab import config

con = duckdb.connect()
con.execute("SET memory_limit='16GB'")
con.execute("SET threads=8")
con.execute("INSTALL mysql"); con.execute("LOAD mysql")
con.execute(config.mysql_attach())

con.execute("""
COPY (
  SELECT CAST(instrument_id AS INTEGER) AS iid,
         code, name, market, sec_class,
         CAST(first_seen_date AS DATE) AS d_first,
         CAST(last_seen_date  AS DATE) AS d_last
  FROM my.market_instrument
) TO 'E:/pq/instrument.parquet' (FORMAT parquet, COMPRESSION zstd)
""")

con.execute("""
CREATE OR REPLACE TABLE ctx AS
SELECT iid, d, o, c, v, amt,
  AVG(c) OVER w59 AS base59,
  AVG(c) OVER w60 AS prev_ma60,
  LAG(c) OVER pw  AS prev_c,
  LAG(v) OVER pw  AS prev_v,
  COUNT(*) OVER w59 AS n59,
  COUNT(*) OVER w60 AS n60
FROM 'E:/pq/daily.parquet'
WINDOW pw  AS (PARTITION BY iid ORDER BY d),
       w59 AS (PARTITION BY iid ORDER BY d ROWS BETWEEN 59 PRECEDING AND 1 PRECEDING),
       w60 AS (PARTITION BY iid ORDER BY d ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
""")

t = time.time()
r = con.execute("""
SELECT i.code, i.name, i.market,
       x.c AS close_px,
       ROUND(x.base59) AS breakout_px,
       ROUND((x.base59*59 + x.c)/60) AS ma60_today,
       ROUND((x.c/x.prev_c - 1)*100, 2) AS chg_pct,
       ROUND(x.v / x.prev_v, 2) AS vol_ratio,
       ROUND((x.c/x.base59 - 1)*100, 2) AS over_pct
FROM ctx x
JOIN 'E:/pq/instrument.parquet' i ON i.iid = x.iid
WHERE x.d = (SELECT MAX(d) FROM ctx)
  AND x.n59=59 AND x.n60=60
  AND i.sec_class = 'COMMON'
  AND i.d_first <= x.d AND i.d_last >= x.d
  AND x.prev_c < x.prev_ma60
  AND x.c > x.base59
  AND x.c > x.o
  AND x.c > x.prev_c
  AND x.c/x.prev_c - 1 BETWEEN 0.02 AND 0.12
  AND x.v > x.prev_v * 1.5
  AND x.c >= 1000 AND x.amt >= 1000000000
ORDER BY over_pct
""").fetchall()
print(f"scan: {time.time()-t:.3f}s  hits: {len(r)}")
for row in r: print(row)
