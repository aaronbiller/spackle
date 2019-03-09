import mock
import os

from google.cloud.bigquery.table import Row, RowIterator

from spackl.db.base import BaseDb
from spackl.db.bigquery import BigQuery, BIGQUERY_DEFAULT_CONN_KWARGS
from spackl.util import Path

uname = os.uname()[1]
project = 'my-project'
expected_conn_kwargs = {'project': 'my-project', 'credentials': None, 'location': None}
query = 'select * from nowhere'
test_creds_path = Path.cwd().as_posix() + '/tests/db/configs/bq_creds.json'
test_creds_not_exist_path = Path.cwd().as_posix() + '/tests/configs/bq_creds_nope.json'
expected_query_results = [{'first': 'a', 'second': 'b', 'third': 'c'},
                          {'first': 'd', 'second': 'e', 'third': 'f'},
                          {'first': 'g', 'second': 'h', 'third': 'i'}]


class MockBigQueryTable(object):
    def __init__(self, dataset):
        self.dataset = dataset

    @property
    def table_id(self):
        return '{}.table'.format(self.dataset)


class MockBigQueryClient(object):
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def set_creds_var(self):
        self.creds_var = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)

    def query(self, sql, **kwargs):
        self._query = query
        return self

    def result(self):
        result = mock.MagicMock(spec=RowIterator)
        result.__iter__.return_value = [
            Row(['a', 'b', 'c'], {'first': 0, 'second': 1, 'third': 2}),
            Row(['d', 'e', 'f'], {'first': 0, 'second': 1, 'third': 2}),
            Row(['g', 'h', 'i'], {'first': 0, 'second': 1, 'third': 2}),
        ]
        return result

    def dataset(self, dataset):
        self._dataset = dataset
        return self

    def list_tables(self, mbqc):
        return [MockBigQueryTable(mbqc._dataset) for i in range(3)]

    def table(self, table_id):
        self._table = table_id
        return table_id

    def delete_table(self, table_id):
        return None


def test_bigquery():
    bq1 = BigQuery()

    assert isinstance(bq1, BaseDb)
    assert bq1._name is None
    assert bq1._db_type is None
    assert bq1.project is None


def test_with_kwargs():
    name = 'Alphabet'
    bq1 = BigQuery(name)
    assert str(bq1) == '<BigQuery(None)>'
    assert bq1.name == name

    bq2 = BigQuery(project=project)
    assert str(bq2) == '<BigQuery({})>'.format(project)
    assert bq2.project == project
    assert bq2._conn_kwargs == expected_conn_kwargs

    bq3 = BigQuery()
    assert str(bq3) == '<BigQuery(None)>'
    bq3.project = project
    assert str(bq3) == '<BigQuery({})>'.format(project)

    bq4 = BigQuery(genuflect='the-vatican-rag', locution='DE')
    assert bq4._conn_kwargs == BIGQUERY_DEFAULT_CONN_KWARGS


def test_connection():
    with mock.patch.dict('spackl.db.bigquery.os.environ', {'BIGQUERY_CREDS_FILE': ''}):
        bq = BigQuery(project=project)

    assert bq._conn is None
    assert bq.connected is False

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        bq.connect()

    assert bq._conn
    assert bq.connected is True
    assert bq._conn.project == project

    results = bq.query(query)
    assert results.result == expected_query_results

    bq.close()
    assert not bq._conn
    assert bq.connected is False


def test_connection_with_env():
    try:
        del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except KeyError:
        pass

    with mock.patch.dict('spackl.db.bigquery.os.environ', {'BIGQUERY_CREDS_FILE': test_creds_path}):
        bq = BigQuery()

    assert bq._conn is None
    assert bq.connected is False

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
        bq._conn.set_creds_var()

    assert bq._conn
    assert bq.connected is True
    assert bq._conn.creds_var == test_creds_path


def test_connection_with_bad_env():
    try:
        del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except KeyError:
        pass

    with mock.patch.dict(
            'spackl.db.bigquery.os.environ', {'BIGQUERY_CREDS_FILE': test_creds_not_exist_path}):
        bq = BigQuery()

    assert bq._conn is None
    assert bq.connected is False

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
        bq._conn.set_creds_var()

    assert bq._conn
    assert bq.connected is True
    assert bq._conn.creds_var is None


def test_connection_with_passed():
    try:
        del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    except KeyError:
        pass

    bq = BigQuery(creds_file=test_creds_path)
    assert bq._bq_creds_file == test_creds_path
    assert bq._conn_kwargs == BIGQUERY_DEFAULT_CONN_KWARGS

    assert bq._conn is None
    assert bq.connected is False

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
        bq._conn.set_creds_var()

    assert bq._conn
    assert bq.connected is True
    assert bq._conn.creds_var == test_creds_path


def test_query_without_connection():
    bq = BigQuery()

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        bq.query(query)
    assert bq._conn
    assert bq.connected is True


def test_query_vs_execute():
    bq = BigQuery()

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        res = bq.query(query)
    assert res is not None

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        res = bq.execute(query)
    assert res is None


def test_list_tables():
    bq = BigQuery()

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
    tables = bq.list_tables('my_dataset')
    assert tables == ['my_dataset.table'] * 3

    bq1 = BigQuery()

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        tables1 = bq1.list_tables('my_dataset')
    assert tables1 == ['my_dataset.table'] * 3
    assert bq1.connected is True


def test_delete_table():
    bq = BigQuery()

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        bq.connect()
    res = bq.delete_table('my_dataset', 'my_table')
    assert res is None
    assert bq._conn._dataset == 'my_dataset'
    assert bq._conn._table == 'my_table'

    bq1 = BigQuery()

    with mock.patch('spackl.db.bigquery.Client', MockBigQueryClient):
        bq1.delete_table('my_dataset', 'my_table')
    assert bq1.connected is True
