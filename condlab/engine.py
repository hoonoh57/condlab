import duckdb
from . import config

class Engine:
    def __init__(self, read_only_store: bool = False):
        self.con = duckdb.connect()
        self.con.execute(f"SET memory_limit='{config.DUCK_MEMORY}'")
        self.con.execute(f"SET threads={config.DUCK_THREADS}")

    def sql(self, q: str, params: list | None = None):
        return self.con.execute(q, params or [])

    def df(self, q: str, params: list | None = None):
        return self.con.execute(q, params or []).fetchdf()

    def rows(self, q: str, params: list | None = None) -> list[dict]:
        cur = self.con.execute(q, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def close(self):
        self.con.close()

    def __enter__(self):  return self
    def __exit__(self, *a): self.close()