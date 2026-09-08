import duckdb, time

con = duckdb.connect()
con.execute("SET memory_limit='16GB'")
con.execute("SET threads=8")

t = time.time()
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
print(f"ctx build: {time.time()-t:.2f}s")
print(con.execute("SELECT COUNT(*) FROM ctx WHERE n59=59 AND n60=60").fetchall())

t = time.time()
r = con.execute("""
SELECT d, COUNT(*) AS hits
FROM ctx
WHERE n59=59 AND n60=60
  AND prev_c < prev_ma60
  AND c > base59
  AND c > o
  AND c > prev_c
  AND c/prev_c - 1 BETWEEN 0.02 AND 0.12
  AND v > prev_v * 1.5
  AND c >= 1000 AND amt >= 1000000000
  AND d >= DATE '2026-08-01'
GROUP BY d ORDER BY d
""").fetchall()
print(f"scan: {time.time()-t:.3f}s")
for row in r: print(row)
