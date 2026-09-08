import duckdb

from condlab import config

con = duckdb.connect()
con.execute("INSTALL mysql"); con.execute("LOAD mysql")
con.execute(config.mysql_attach())


def q(sql):
    return con.execute(f"SELECT * FROM mysql_query('my', $${sql}$$)").fetchall()


print("---- estimated size ----")
for r in q("""
SELECT table_name, engine, table_rows,
       ROUND(data_length/1024/1024) AS data_mb,
       ROUND(index_length/1024/1024) AS idx_mb
FROM information_schema.tables
WHERE table_schema='market_data'
"""):
    print(r)

print("---- indexes on minute table ----")
for r in q("""
SELECT index_name, seq_in_index, column_name
FROM information_schema.statistics
WHERE table_schema='market_data'
  AND table_name='korean_equity_minute_1m'
ORDER BY index_name, seq_in_index
"""):
    print(r)

print("---- date extent (indexed probe) ----")
for r in q("""
SELECT MIN(trading_date) AS d_min, MAX(trading_date) AS d_max
FROM market_data.korean_equity_minute_1m
"""):
    print(r)