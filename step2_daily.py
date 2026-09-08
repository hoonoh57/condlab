import duckdb, time

from condlab import config

con = duckdb.connect()
con.execute("SET memory_limit='16GB'")
con.execute("SET threads=8")
con.execute("INSTALL mysql")
con.execute("LOAD mysql")
con.execute(config.mysql_attach())

t = time.time()
con.execute("""
COPY (
  SELECT CAST(instrument_id AS INTEGER) AS iid,
         CAST(trading_date  AS DATE)    AS d,
         CAST(open   AS INTEGER) AS o,
         CAST(high   AS INTEGER) AS h,
         CAST(low    AS INTEGER) AS l,
         CAST(close  AS INTEGER) AS c,
         CAST(volume AS BIGINT)  AS v,
         CAST(amount AS BIGINT)  AS amt
  FROM my.korean_equity_daily
  ORDER BY instrument_id, trading_date
) TO 'E:/pq/daily.parquet' (FORMAT parquet, COMPRESSION zstd)
""")
print(f"export: {time.time()-t:.1f}s")

print(con.execute("""
SELECT COUNT(*) n, COUNT(DISTINCT iid) codes, MIN(d) d_min, MAX(d) d_max
FROM 'E:/pq/daily.parquet'
""").fetchall())
