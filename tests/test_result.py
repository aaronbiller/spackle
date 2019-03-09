import datetime
import decimal
import json
import pandas as pd
import pytest
import types

from collections import OrderedDict

from spackl.result import (
    BaseResult,
    ResultCol,
    ResultRow)
from spackl.util import DtDecEncoder


def generate_result():
    keys = ['a', 'b', 'c']
    result = [OrderedDict([('a', 1), ('b', decimal.Decimal(2.0)), ('c', datetime.date(2018, 8, 1))]),
              OrderedDict([('a', 4), ('b', decimal.Decimal(5.0)), ('c', datetime.date(2018, 9, 1))]),
              OrderedDict([('a', 7), ('b', decimal.Decimal(8.0)), ('c', datetime.datetime(2018, 10, 1))])]
    return keys, result


def generate_other_result():
    keys = ['a', 'b']
    result = [OrderedDict([('a', 1), ('b', 2)]),
              OrderedDict([('a', 3), ('b', 4)])]
    return keys, result


expected_keys, expected_result = generate_result()
expected_other_keys, expected_other_result = generate_other_result()


def test_baseresult():
    with pytest.raises(TypeError):
        BaseResult('nope')

    with pytest.raises(TypeError):
        BaseResult(dict(), list())

    bq = BaseResult(['a'], 'a')
    assert bq._result == ['a']

    with pytest.raises(TypeError):
        BaseResult(expected_result)

    br = BaseResult(list(), list())
    assert isinstance(br.keys(), types.GeneratorType)
    assert bool(br) is False

    br = BaseResult(*generate_result())

    assert bool(br) is True
    assert not br == 3
    assert br != 3
    assert len(br) == 3
    for row in br:
        assert isinstance(row, ResultRow)

    assert br.a == ResultCol('a', (1, 4, 7))
    assert br['a'] == ResultCol('a', (1, 4, 7))
    assert isinstance(br.a, ResultCol)
    assert isinstance(br[0], ResultRow)
    with pytest.raises(TypeError):
        br[OrderedDict]

    expected_list = [(1, decimal.Decimal('2'), datetime.date(2018, 8, 1)),
                     (4, decimal.Decimal('5'), datetime.date(2018, 9, 1)),
                     (7, decimal.Decimal('8'), datetime.datetime(2018, 10, 1, 0, 0))]
    expected_json = json.dumps(expected_result, cls=DtDecEncoder)
    expected_df = pd.DataFrame(expected_result)

    assert br.list() == expected_list
    assert [v for v in br.values()] == expected_list
    assert br.json() == expected_json
    assert br.df().equals(expected_df)
    assert str(br) == expected_json

    expected_items = {'a': (1, 4, 7),
                      'b': (decimal.Decimal(2.0), decimal.Decimal(5.0), decimal.Decimal(8.0)),
                      'c': (datetime.date(2018, 8, 1), datetime.date(2018, 9, 1), datetime.datetime(2018, 10, 1))}
    for k, v in br.items():
        assert expected_items[k] == v

    assert br.get('a') == ResultCol('a', (1, 4, 7))
    assert br.get(1) is None
    assert br.get('d') is None

    assert isinstance(br.keys(), types.GeneratorType)
    expected_keys = ['a', 'b', 'c']
    for i, k in enumerate(br.keys()):
        assert k == expected_keys[i]

    f = br.first()
    assert str(f) == str(expected_list[0])
    assert isinstance(f, ResultRow)


def test_baseresult_manipulation():
    br = BaseResult(*generate_result())
    br2 = BaseResult(*generate_other_result())

    with pytest.raises(NotImplementedError):
        br.append(5)
    br.append(br[0])
    assert len(br) == 4
    assert br[-1] == br[0]
    with pytest.raises(ValueError):
        br.append(br2[0])

    with pytest.raises(NotImplementedError):
        br.extend([5, 6, 7])
    br.extend(br)
    assert len(br) == 8
    assert br[:4] == br[4:]
    with pytest.raises(ValueError):
        br.extend(br2)

    empty_br = BaseResult(list(), list())
    empty_br.append(br[0])
    assert empty_br._keys == expected_keys
    assert len(empty_br) == 1

    empty_br2 = BaseResult(list(), list())
    empty_br2.extend(br2)
    assert empty_br2._keys == expected_other_keys
    assert len(empty_br2) == 2

    filtered = br.filter(lambda row: row.a < 7)
    assert isinstance(filtered, BaseResult)
    assert filtered is not br
    assert len(filtered) == 6

    br.filter(lambda row: row.a < 4, inplace=True)
    assert len(br) == 4

    r = br.pop()
    assert isinstance(r, ResultRow)
    assert len(br) == 3

    br.append(filtered.filter(lambda row: row.a > 1)[0])
    br.extend(br)
    r = br.pop(3)
    assert r.a == 4


def test_resultrow():
    br = BaseResult(*generate_result())
    rr = br[0]

    assert bool(rr) is True

    assert rr[0] == rr.a == rr['a'] == 1
    assert rr[1] == rr.b == rr['b'] == decimal.Decimal(2.0)
    assert rr[2] == rr.c == rr['c'] == datetime.date(2018, 8, 1)
    with pytest.raises(TypeError):
        rr[OrderedDict]
    with pytest.raises(KeyError):
        rr['d']

    assert not rr == 3
    assert rr != 3

    assert rr.keys() == expected_keys
    assert rr.values() == (1, decimal.Decimal('2'), datetime.date(2018, 8, 1))
    assert str(rr) == "(1, Decimal('2'), datetime.date(2018, 8, 1))"

    expected_items = {'a': 1, 'b': decimal.Decimal(2.0), 'c': datetime.date(2018, 8, 1)}
    for k, v in rr.items():
        assert expected_items[k] == v

    assert rr.get('a') == 1
    assert rr.get(1) is None
    assert rr.get('d') is None


def test_resultcol():
    br = BaseResult(*generate_result())
    rc = br.a

    assert bool(rc) is True

    assert rc.a == rc['a'] == (1, 4, 7)
    assert rc[0] == 1
    assert rc[1] == 4
    assert rc[2] == 7
    with pytest.raises(TypeError):
        rc[OrderedDict]
    with pytest.raises(KeyError):
        rc['b']
    with pytest.raises(IndexError):
        rc[3]

    assert not rc == 3
    assert rc != 3
    assert len(rc) == 3

    assert rc._key == 'a'
    assert str(rc) == '(1, 4, 7)'

    assert str(ResultCol('a', ('one', 'two', 'three'))) == "('one', 'two', 'three')"
    assert str(ResultCol(u'a', (u'one', u'two', u'three'))) == "('one', 'two', 'three')"

    expected_vals = (1, 4, 7)
    for i, v in enumerate(rc):
        assert v == expected_vals[i]

    assert rc[0:1] == expected_vals[0:1]


def test_resultcol_rquery_format():
    br = BaseResult(*generate_result())

    assert br['a']._rquery_format() == '(1, 4, 7)'

    rc2 = ResultCol('a', ('one', None, 'three'))
    assert rc2._rquery_format() == "('one', 'three')"

    rc3 = ResultCol('a', (None, ))
    assert rc3._rquery_format() == "('__xxx__EMPTYRESULT__xxx__')"

    rc4 = ResultCol('a', (1, None))
    assert rc4._rquery_format() == '(1)'

    rc5 = ResultCol('a', (None, 'one', None))
    assert rc5._rquery_format() == "('one')"
