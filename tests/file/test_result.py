import datetime
import decimal
import mock
import pytest
import types

from collections import OrderedDict

from spackl.result import ResultRow, ResultCol
from spackl.file.result import FileResult

results = [OrderedDict([('a', 1), ('b', decimal.Decimal(2.0)), ('c', datetime.date(2018, 8, 1))]),
           OrderedDict([('a', 4), ('b', decimal.Decimal(5.0)), ('c', datetime.date(2018, 9, 1))]),
           OrderedDict([('a', 7), ('b', decimal.Decimal(8.0)), ('c', datetime.datetime(2018, 10, 1))])]
malformed_results = [OrderedDict([('a', 1), ('b', 2)]),
                     OrderedDict([('a', 3), ('c', 4)])]


def get_mock_iterator(spec, values):
    mock_result = mock.MagicMock(spec=spec)
    mock_result.__iter__.return_value = values
    return mock_result


def test_fileresult():
    with pytest.raises(TypeError):
        FileResult('nope')

    fr = FileResult()
    assert bool(fr) is False
    assert fr._result == list()

    with pytest.raises(AttributeError):
        FileResult(malformed_results)

    fr = FileResult(list())
    assert isinstance(fr.keys(), types.GeneratorType)
    assert bool(fr) is False

    fr = FileResult(results)

    assert bool(fr) is True
    assert not fr == 3
    assert fr != 3
    assert len(fr) == 3
    for row in fr:
        assert isinstance(row, ResultRow)

    assert fr.a == ResultCol('a', (1, 4, 7))
    assert fr['a'] == ResultCol('a', (1, 4, 7))
    assert isinstance(fr.a, ResultCol)
    assert isinstance(fr[0], ResultRow)

    expected_keys = ['a', 'b', 'c']
    for i, k in enumerate(fr.keys()):
        assert k == expected_keys[i]
