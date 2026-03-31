import os
import pymysql
import pymysql.cursors
from typing import Literal
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

DB_CONFIGS = {
    "source": {
        "host":     os.getenv("SRC_HOST",     "localhost"),
        "port":     int(os.getenv("SRC_PORT", "3306")),
        "user":     os.getenv("SRC_USER",     "source_username"),
        "password": os.getenv("SRC_PASSWORD", "source_password"),
        "database": "vehicle_manufacturing_src",
    },
    "target": {
        "host":     os.getenv("TGT_HOST",     "localhost"),
        "port":     int(os.getenv("TGT_PORT", "3306")),
        "user":     os.getenv("TGT_USER",     "target_username"),
        "password": os.getenv("TGT_PASSWORD", "target_password"),
        "database": "vehicle_manufacturing_dw",
    },
}

CONNECT_KWARGS = {
    "charset":       "utf8mb4",
    "cursorclass":   pymysql.cursors.DictCursor,   # rows as dicts, not tuples
    "connect_timeout": 10,
    "autocommit":    False,
}

# auto commit is off so need to call commit, auto close is also off

def get_connection(db: Literal["source", "target"] = "source") -> pymysql.connections.Connection:

    if db not in DB_CONFIGS:
        raise ValueError(f"db must be 'source' or 'target', got {db!r}")

    cfg = {**DB_CONFIGS[db], **CONNECT_KWARGS}
    conn = pymysql.connect(**cfg)
    return conn


def get_engine(db: Literal["source", "target"] = "source") -> Engine:

    if db not in DB_CONFIGS:
        raise ValueError(f"db must be 'source' or 'target', got {db!r}")

    cfg = {**DB_CONFIGS[db], **CONNECT_KWARGS}
    print(f"Connecting to {db} with:")
    print(f"  Host: {cfg.get('host')}")
    print(f"  User: {cfg.get('user')}")
    print(f"  Database: {cfg.get('database')}")
    print(f"  Port: {cfg.get('port', 3306)}")
    print(f"  Password: {'*' * len(cfg.get('password', ''))}")  # Show length but not actual password

    db_url = (
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg.get('port', 3306)}/{cfg['database']}"
    )

    # Create and return engine
    engine = create_engine(
        db_url,
        pool_pre_ping=True,  # Verify connections before using
        pool_recycle=3600,  # Recycle connections every hour
        echo=False  # Set to True for SQL debugging
    )
    return engine

def run_custom(local_engine:Engine):
    from sqlalchemy import text
    with local_engine.connect() as conn:
        cursor = conn.execute(text("SELECT * FROM suppliers"))
        print(cursor.fetchall())

        conn.commit()
    pass

if __name__=="__main__":
    engine=get_engine("source")
    print(engine)
    run_custom(engine)
