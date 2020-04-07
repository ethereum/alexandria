import pathlib
import tempfile

import pytest

from alexandria.durable_db import DurableDB


@pytest.fixture
def db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield pathlib.Path(temp_dir)


@pytest.fixture
def db(db_path):
    return DurableDB(db_path)


def test_durable_db_setting(db):
    with pytest.raises(KeyError):
        db.get(b'key-a')
    db.set(b'key-a', b'value-a-original')
    assert db.get(b'key-a') == b'value-a-original'
    db.set(b'key-a', b'value-a-updated')
    assert db.get(b'key-a') == b'value-a-updated'
    db.delete(b'key-a')
    with pytest.raises(KeyError):
        db.get(b'key-a')


def test_durable_db_key_iteration(db):
    db.set(b'key-a', b'value-a')
    db.set(b'key-b', b'value-b')
    db.set(b'key-c', b'value-c')
    db.set(b'key-d', b'value-d')
    keys = tuple(db.keys())
    assert keys == (b'key-a', b'key-b', b'key-c', b'key-d')
