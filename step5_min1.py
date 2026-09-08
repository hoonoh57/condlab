"""Extract minute bars from MySQL to partitioned parquet."""
import sys
import time

import duckdb

from condlab import config

D_FROM, D_TO = sys.argv[1], sys.argv[2]
OUT = config.MIN1_DIR.as_posix()
TMP = (config.PQ_DIR / "tmp").as_posix()

con = duckdb.connect()
con.execute("SET memory_limit='12GB'")
con.execute(f"SET threads={config.DUCK_THREADS}")
con.execute(f"SET temp_directory='{TMP}'")
con.execute("INSTALL mysql")
con.execute("LOAD mysql")
con.execute(config.mysql_attach())

src = f"""
SELECT instrument_id, trading_date, bar_timestamp,
       open, high, low, close, volume
FROM market_data.korean_equity_minute_1m
     FORCE INDEX (idx_korean_equity_minute_1m_date)
WHERE trading_date BETWEEN '{D_FROM}' AND '{D_TO}'
"""

t = time.time()
con.execute(f"""
COPY (
  SELECT CAST(instrument_id  AS INTEGER)   AS iid,
         CAST(trading_date   AS DATE)      AS d,
         CAST(bar_timestamp  AS TIMESTAMP) AS ts,
         CAST(open   AS INTEGER) AS o,
         CAST(high   AS INTEGER) AS h,
         CAST(low    AS INTEGER) AS l,
         CAST(close  AS INTEGER) AS c,
         CAST(volume AS BIGINT)  AS v
  FROM mysql_query('my', $${src}$$)
  ORDER BY d, iid, ts
) TO '{OUT}' (
  FORMAT parquet, PARTITION_BY (d), COMPRESSION zstd,
  ROW_GROUP_SIZE 100000, APPEND
)
""")
elapsed = time.time() - t

n = con.execute(f"""
SELECT COUNT(*), COUNT(DISTINCT iid), COUNT(DISTINCT d), MIN(d), MAX(d)
FROM read_parquet('{OUT}/**/*.parquet', hive_partitioning=true)
WHERE d BETWEEN DATE '{D_FROM}' AND DATE '{D_TO}'
""").fetchone()

print(f"{D_FROM}~{D_TO}  export {elapsed:.1f}s")
print(f"rows={n[0]:,}  codes={n[1]}  days={n[2]}  {n[3]} ~ {n[4]}")