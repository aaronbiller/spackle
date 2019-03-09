import mock
import pytest

from spackl.file.base import BaseFile


def test_abc():
    with pytest.raises(TypeError):
        BaseFile()


def test_base_db():
    bf = BaseFile
    bf.__abstractmethods__ = set()
    bf = bf()

    assert bf._data is None
    assert bf.opened is False

    with pytest.raises(NotImplementedError):
        bf.open()
    assert bf._data is None
    assert bf.opened is False

    bf._data = 'lookatmydataz'
    with mock.patch.object(bf, '_open'):
        bf.open()
    assert bf.opened is True
    with mock.patch.object(bf, '_open'):
        bf.open()
    assert bf.opened is True

    with mock.patch.object(bf, '_close'):
        bf.close()
    assert bf._data is None
    assert bf.opened is False

    with mock.patch.object(bf, '_open'):
        bf.open()
    assert bf.opened is False

    with mock.patch.object(bf, '_close'):
        bf.close()

    with pytest.raises(NotImplementedError):
        bf.query()

    with pytest.raises(NotImplementedError):
        bf._close()
