import mock
import os

from spackl.db.base import BaseDb
from spackl.db import Postgres, Redshift

uname = os.uname()[1]
expected_db_type = 'redshift+psycopg2'
expected_default_url = '{0}://{1}@localhost:5439/{1}'.format(expected_db_type, uname)
query = 'select * from nowhere'
expected_query_results = [{'first': 'a', 'second': 'b', 'third': 'c'},
                          {'first': 'd', 'second': 'e', 'third': 'f'},
                          {'first': 'g', 'second': 'h', 'third': 'i'}]


def test_redshift():
    rs = Redshift()

    assert isinstance(rs, BaseDb)
    assert isinstance(rs, Postgres)
    assert rs._conn_kwargs
    assert rs._conn_kwargs['port'] == 5439
    assert rs._db_type == expected_db_type

    rs1 = Redshift(port=1234)
    assert rs1._conn_kwargs['port'] == 1234


def test_postgres_and_redshift():
    rs = Redshift(name='red')
    pg = Postgres(name='blue')

    assert str(rs) == '<Redshift(localhost)>'
    assert str(pg) == '<Postgres(localhost)>'

    assert rs._conn_kwargs
    assert rs._conn_kwargs['port'] == 5439
    assert rs._db_type == expected_db_type

    assert pg._conn_kwargs
    assert pg._conn_kwargs['port'] == 5432
    assert pg._db_type == 'postgresql'


def test_connection(mock_create_engine):
    with mock.patch('spackl.db.postgres.sqlalchemy.create_engine', mock_create_engine):
        rs = Redshift()

    assert rs._engine.url == expected_default_url
    assert rs._conn is None
    assert rs.connected is False

    rs.connect()
    assert rs._conn
    assert rs.connected is True

    results = rs.query(query)
    assert results.result == expected_query_results
    assert not rs._conn
    assert rs.connected is False

    rs.connect()
    rs.close()
    assert not rs._conn
    assert rs.connected is False
