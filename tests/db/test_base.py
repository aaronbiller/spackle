import mock
import pytest

from spackl.db.base import BaseDb


def test_abc():
    with pytest.raises(TypeError):
        BaseDb()


def test_base_db():
    db = BaseDb
    db.__abstractmethods__ = set()
    db = db()

    assert db._conn is None
    assert db.connected is False

    with pytest.raises(NotImplementedError):
        db.connect()
    assert db._conn is None
    assert db.connected is False

    db._conn = 'immaconnection'
    with mock.patch.object(db, '_connect'):
        db.connect()
    assert db.connected is True

    with mock.patch.object(db, '_close'):
        db.close()
    assert db._conn is None
    assert db.connected is False

    with mock.patch.object(db, '_connect'):
        db.connect()
    assert db.connected is False

    with mock.patch.object(db, '_close'):
        db.close()

    with pytest.raises(TypeError):
        db.query()

    with pytest.raises(TypeError):
        db.execute()

    with pytest.raises(NotImplementedError):
        db.query('select 1')

    with pytest.raises(NotImplementedError):
        db.execute('insert 1')

    with pytest.raises(NotImplementedError):
        db._close()
