import datetime
import decimal
import json
import pytest

from collections import OrderedDict

from spackl.util import DtDecEncoder

results = [OrderedDict([('a', 1), ('b', decimal.Decimal(2.0)), ('c', datetime.date(2018, 8, 1))]),
           OrderedDict([('a', 4), ('b', decimal.Decimal(5.0)), ('c', datetime.date(2018, 9, 1))]),
           OrderedDict([('a', 7), ('b', decimal.Decimal(8.0)), ('c', datetime.datetime(2018, 10, 1))])]


def test_dtdecencoder():
    with pytest.raises(TypeError):
        json.dumps(results)

    expected_json = (
        '[{"a": 1, "b": 2.0, "c": "2018-08-01"}, '
        '{"a": 4, "b": 5.0, "c": "2018-09-01"}, '
        '{"a": 7, "b": 8.0, "c": "2018-10-01T00:00:00"}]')
    data = json.dumps(results, cls=DtDecEncoder, sort_keys=True)
    assert data == expected_json

    bad_json = {'a': 7, 'b': decimal.Decimal(8.0), 'c': datetime.datetime(2018, 10, 1), 'd': OrderedDict}
    with pytest.raises(TypeError):
        json.dumps(bad_json, cls=DtDecEncoder, sort_keys=True)
