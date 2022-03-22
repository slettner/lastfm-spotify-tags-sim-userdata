import functools
import pathlib
import sqlite3
from functools import wraps
from typing import Union

DB_PATH: Union[None, str, pathlib.Path] = None


def init(path: Union[str, pathlib.Path]):
    global DB_PATH
    DB_PATH = path


def row_factory(cur, row):
    return dict(sqlite3.Row(cur, row))


def maybe_wrap_connection(func):
    """Checks if `DB_PATH` is set and if yes return a partial function
    with the set connection. If not it just calls the function normally.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """A wrapper function"""
        global DB_PATH
        if DB_PATH is not None:
            with sqlite3.connect(DB_PATH) as con:
                con.row_factory = row_factory
                if "con" in kwargs:
                    kwargs.pop("con")
                if len(args) > 0 and isinstance(args[0], sqlite3.Connection):
                    args = args[1:]
                return func(con, *args, **kwargs)
        else:
            return func(*args, **kwargs)

    return wrapper
