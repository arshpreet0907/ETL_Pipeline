"""
sf_connector.py
---------------
Snowflake connection factory for the ETL pipeline.
Returns a SQLAlchemy engine connected to the configured Snowflake account.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")


def get_snowflake_engine() -> Engine:
    account   = os.getenv("SF_ACCOUNT")
    user      = os.getenv("SF_USER")
    password  = os.getenv("SF_PASSWORD")
    database  = os.getenv("SF_DATABASE")
    schema    = os.getenv("SF_SCHEMA")
    warehouse = os.getenv("SF_WAREHOUSE")
    role      = os.getenv("SF_ROLE", "")

    if not all([account, user, password, database, schema, warehouse]):
        raise EnvironmentError(
            "Missing one or more required Snowflake env vars. "
            "Check snowflake/.env — see setup.txt for where to find them."
        )

    engine = create_engine(
        URL(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role or None,
        ),
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False,
    )

    return engine

def get_account_details(snow_engine:Engine):
    with snow_engine.connect() as conn:
        result = conn.execute(text(
            "SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_ROLE()"
        ))
        row = result.fetchone()
        print(f"  User      : {row[0]}")
        print(f"  Warehouse : {row[1]}")
        print(f"  Database  : {row[2]}")
        print(f"  Role      : {row[3]}")

def run_custom(snow_engine:Engine):
    with snow_engine.connect() as conn:
        cursor = conn.execute(text("SELECT COUNT(*) FROM etl_run_log"))
        print(cursor.fetchall())

        conn.commit()

if __name__ == "__main__":
    engine = get_snowflake_engine()
    print(f"Snowflake connection engine: {engine}")
    # get_account_details(engine)
    run_custom(engine)


