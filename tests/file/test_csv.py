import mock
import pytest

from collections import OrderedDict
from io import StringIO
import pandas as pd
from pandas.util.testing import assert_frame_equal

from spackl.file.base import BaseFile
from spackl.file import CSV
from spackl.util import Path

test_csv_path = Path.cwd().as_posix() + '/tests/file/data/test.csv'
test_csv_tab_path = Path.cwd().as_posix() + '/tests/file/data/test_tab.csv'
test_csv_zip_path = Path.cwd().as_posix() + '/tests/file/data/test.csv.zip'
test_nonexistent_csv_path = 'tests/file/data/test_nothere_config.csv'
expected_results = [OrderedDict([('first', 'a'), ('second', 'b'), ('third', 'c')]),
                    OrderedDict([('first', 'd'), ('second', 'e'), ('third', 'f')]),
                    OrderedDict([('first', 'g'), ('second', 'h'), ('third', 'i')])]


def test_csv():
    with pytest.raises(TypeError):
        CSV()

    with pytest.raises(AttributeError):
        CSV(test_nonexistent_csv_path)

    with pytest.raises(AttributeError):
        CSV(dict())

    csv = CSV(test_csv_path)
    assert isinstance(csv, BaseFile)
    assert csv._name is None
    assert csv._use_pandas is False
    assert csv._csv_kwargs == dict()


def test_with_kwargs():
    name = 'Euphegenia Doubtfire, dear.'
    csv = CSV(test_csv_path, name=name)
    assert str(csv) == test_csv_path
    assert csv.name == name

    dialect = 'excel'
    csv = CSV(test_csv_path, dialect=dialect)
    assert csv._csv_kwargs == {'dialect': 'excel'}


def test_filelike_obj():
    obj = StringIO()
    csv = CSV(obj)

    assert str(csv) == str(obj)
    assert csv._data is None
    assert csv.opened is False

    csv.open()
    assert csv._data
    assert csv.opened is True

    csv.close()
    assert not csv._data
    assert csv.opened is False


def test_file():
    csv = CSV(test_csv_path)

    csv.open()
    assert csv._data
    assert csv.opened is True

    results = csv.query()
    assert results.result == expected_results

    csv.close()
    assert not csv._data
    assert csv.opened is False


def test_tab_file():
    csv = CSV(test_csv_tab_path)

    results = csv.query(dialect='excel-tab')
    assert results.result == expected_results
    assert not csv._data
    assert csv.opened is False


def test_zipfile():
    csv = CSV(test_csv_zip_path)

    csv.open()
    assert csv._data
    assert csv.opened is True

    results = csv.query()
    assert results.result == expected_results

    csv.close()
    assert not csv._data
    assert csv.opened is False


def test_pandas():
    csv = CSV(test_csv_path, use_pandas=True)

    results = csv.query()
    expected_df = pd.DataFrame(expected_results)
    assert_frame_equal(results, expected_df)


def test_fail_close():
    csv = CSV(test_csv_zip_path)
    csv.open()
    with mock.patch.object(csv._data, 'close', side_effect=Exception('Zoinks!')):
        with mock.patch('spackl.file.csv._log.warning') as mock_warn:
            csv.close()
    mock_warn.assert_called_once()
