import duckdb

con = duckdb.connect()
rows = con.execute(
    "DESCRIBE SELECT * FROM 'E:/pq/instrument.parquet'"
).fetchall()
for r in rows:
    print(r[0], "|", r[1])

print("---- sample ----")
for r in con.execute(
    "SELECT * FROM 'E:/pq/instrument.parquet' LIMIT 3"
).fetchall():
    print(r)