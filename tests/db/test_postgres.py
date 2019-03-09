import mock
import os
import pytest

from spackl.db.base import BaseDb, get_default_db_conn_kwargs
from spackl.db import Postgres

uname = os.uname()[1]
expected_default_url = 'postgresql://{0}@localhost:5432/{0}'.format(uname)
query = 'select * from nowhere'
expected_query_results = [{'first': 'a', 'second': 'b', 'third': 'c'},
                          {'first': 'd', 'second': 'e', 'third': 'f'},
                          {'first': 'g', 'second': 'h', 'third': 'i'}]


def test_postgres():
    pg = Postgres()

    assert isinstance(pg, BaseDb)
    assert pg._name is None
    assert pg._db_type == 'postgresql'


def test_with_kwargs():
    name = 'Euphegenia Doubtfire, dear.'
    pg1 = Postgres(name)
    assert str(pg1) == '<Postgres(localhost)>'
    assert pg1.name == name

    conn_string = 'postgresql://user:pass@host:5432/db'
    pg2 = Postgres(conn_string=conn_string)
    pg2._conn_kwargs is None
    assert str(pg2) == '<Postgres(host)>'

    host = 'notlocahost'
    pg3 = Postgres(host=host)
    assert pg3._conn_kwargs['host'] == host

    with pytest.raises(ValueError):
        Postgres(conn_string=42)

    pg4 = Postgres(part=5432, horse='localhorse')
    assert pg4._conn_kwargs == get_default_db_conn_kwargs()


def test_connection(mock_create_engine):
    with mock.patch('spackl.db.postgres.sqlalchemy.create_engine', mock_create_engine):
        pg = Postgres()

    assert pg._engine.url == expected_default_url
    assert pg._conn is None
    assert pg.connected is False

    pg.connect()
    assert pg._conn
    assert pg.connected is True

    results = pg.query(query)
    assert results.result == expected_query_results

    pg.close()
    assert not pg._conn
    assert pg.connected is False


def test_query_without_connection(mock_create_engine):
    with mock.patch('spackl.db.postgres.sqlalchemy.create_engine', mock_create_engine):
        pg = Postgres()

    pg.query(query)
    assert pg._conn is None
    assert pg.connected is False


def test_query_vs_execute(mock_create_engine):
    with mock.patch('spackl.db.postgres.sqlalchemy.create_engine', mock_create_engine):
        pg = Postgres()

    res = pg.query(query)
    assert res is not None

    res = pg.execute(query)
    assert res is None
