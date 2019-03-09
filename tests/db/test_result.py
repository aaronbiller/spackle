import datetime
import decimal
import mock
import pytest
import types

from collections import OrderedDict
from google.cloud.bigquery.table import RowIterator
from sqlalchemy.engine import ResultProxy

from spackl.result import ResultRow, ResultCol
from spackl.db.result import QueryResult

results = [OrderedDict([('a', 1), ('b', decimal.Decimal(2.0)), ('c', datetime.date(2018, 8, 1))]),
           OrderedDict([('a', 4), ('b', decimal.Decimal(5.0)), ('c', datetime.date(2018, 9, 1))]),
           OrderedDict([('a', 7), ('b', decimal.Decimal(8.0)), ('c', datetime.datetime(2018, 10, 1))])]
malformed_results = [OrderedDict([('a', 1), ('b', 2)]),
                     OrderedDict([('a', 3), ('c', 4)])]


def get_mock_iterator(spec, values):
    mock_result = mock.MagicMock(spec=spec)
    mock_result.__iter__.return_value = values
    return mock_result


def test_queryresult():
    with pytest.raises(TypeError):
        QueryResult('nope')

    qr = QueryResult()
    assert bool(qr) is False
    assert qr._result == list()

    itr = get_mock_iterator(RowIterator, malformed_results)
    with pytest.raises(AttributeError):
        QueryResult(itr)

    itr = get_mock_iterator(RowIterator, list())
    qr = QueryResult(itr)
    assert isinstance(qr.keys(), types.GeneratorType)
    assert bool(qr) is False

    itr = get_mock_iterator(ResultProxy, results)
    qr = QueryResult(itr)

    assert bool(qr) is True
    assert not qr == 3
    assert qr != 3
    assert len(qr) == 3
    for row in qr:
        assert isinstance(row, ResultRow)

    assert qr.a == ResultCol('a', (1, 4, 7))
    assert qr['a'] == ResultCol('a', (1, 4, 7))
    assert isinstance(qr.a, ResultCol)
    assert isinstance(qr[0], ResultRow)

    expected_keys = ['a', 'b', 'c']
    for i, k in enumerate(qr.keys()):
        assert k == expected_keys[i]
