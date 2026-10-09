"""Fixtures. The whole suite runs against a throwaway `openaq_test` database, recreated every
session from the same db/*.sql files the real pipeline uses. The dev database is never touched."""
import psycopg
import pytest

from pipeline import config
from pipeline.apply_sql import apply_all
from tests import helpers


@pytest.fixture(scope="session")
def test_db():
    dev = config.db_kwargs()["dbname"]
    assert helpers.TEST_DB != dev, "refusing to run: test DB name equals the dev DB name"
    with psycopg.connect(**config.db_kwargs(dev), autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {helpers.TEST_DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {helpers.TEST_DB}")
    apply_all(dbname=helpers.TEST_DB)
    yield helpers.TEST_DB
    # Left in place after the session on purpose, so you can inspect the dirty data with psql;
    # it is dropped and rebuilt at the start of the next session.


@pytest.fixture
def db(test_db):
    """Clean test database + the 13 fixture sensors in bronze.sensors. Fresh per test."""
    conn = helpers.connect_clean()
    helpers.insert_sensors(conn)
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def gold_sensors(db):
    """Gold tests use many sensor ids; register them in the dimension so Silver does not (correctly)
    reject their readings as ORPHAN_SENSOR. Most never report - which is exactly what sensor_health needs."""
    helpers.insert_sensors(db, range(1014, 1100))
