import json
from condlab import config
from condlab.engine import Engine

print(json.dumps(config.health(), indent=2, ensure_ascii=False))

with Engine() as eng:
    r = eng.rows(f"""
        SELECT COUNT(*) AS n, COUNT(DISTINCT iid) AS codes,
               MIN(d) AS d_min, MAX(d) AS d_max
        FROM '{config.DAILY_PQ.as_posix()}'
    """)
    print(r)