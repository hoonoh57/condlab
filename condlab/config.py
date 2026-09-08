import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv(ROOT / ".env")

PQ_DIR      = Path(os.getenv("CONDLAB_PQ",  r"E:\pq"))
DAILY_PQ    = PQ_DIR / "daily.parquet"
INST_PQ     = PQ_DIR / "instrument.parquet"
MIN1_DIR    = PQ_DIR / "min1"

STORE_DB    = Path(os.getenv("CONDLAB_STORE", str(ROOT / "data" / "condlab.duckdb")))

DUCK_MEMORY = os.getenv("CONDLAB_MEM", "16GB")
DUCK_THREADS= int(os.getenv("CONDLAB_THREADS", "8"))

MYSQL = dict(
    host=os.getenv("CONDLAB_MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("CONDLAB_MYSQL_PORT", "3306")),
    user=os.getenv("CONDLAB_MYSQL_USER", "root"),
    password=os.getenv("CONDLAB_MYSQL_PW", ""),
    database=os.getenv("CONDLAB_MYSQL_DB", "market_data"),
)


def mysql_attach(alias: str = "my") -> str:
    password = MYSQL["password"].replace("'", "''")
    return (
        f"ATTACH 'host={MYSQL['host']} port={MYSQL['port']} "
        f"user={MYSQL['user']} password={password} "
        f"database={MYSQL['database']}' AS {alias} (TYPE mysql, READ_ONLY)"
    )

def health() -> dict:
    return {
        "root": str(ROOT),
        "daily_pq": str(DAILY_PQ),
        "daily_exists": DAILY_PQ.exists(),
        "daily_mb": round(DAILY_PQ.stat().st_size / 1048576, 1) if DAILY_PQ.exists() else None,
        "inst_exists": INST_PQ.exists(),
        "min1_exists": MIN1_DIR.exists(),
        "store": str(STORE_DB),
        "memory_limit": DUCK_MEMORY,
        "threads": DUCK_THREADS,
    }