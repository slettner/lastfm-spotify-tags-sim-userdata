""" Impement """
import sqlite3
import os
from contextlib import contextmanager
from typing import Set, Optional, List

from lastfm_dataset.constants import ROOT_DIR, DATA_DIR
from lastfm_dataset.create.processed_lastfm_data import get_all_tags, populate_tracks_table
from lastfm_dataset.create.utils import row_factory


@contextmanager
def create_database_file(overwrite_existing: bool = False):

    target_path = os.path.join(ROOT_DIR, DATA_DIR, 'dataset.db')
    if os.path.isfile(target_path) and not overwrite_existing:
        raise FileExistsError(target_path)
    con = sqlite3.connect(target_path, isolation_level=None)
    if overwrite_existing:
        drop_all_tables(con)
    con.row_factory = row_factory

    try:
        yield con
    finally:
        con.close()


def create_all_tables(con: sqlite3.Connection):
    tags = get_all_tags()
    create_track_table(con)
    create_users_table(con)
    create_similar_table(con)
    create_tags_table(con, tags)
    create_track_user_table(con)


def drop_all_tables(con: sqlite3.Connection):
    con.execute("""DROP TABLE track_users;""")
    con.execute("""DROP TABLE similar;""")
    # con.execute("""DROP TABLE tags;""")
    con.execute("""DROP TABLE users;""")
    # con.execute("""DROP TABLE tracks;""")


def populate_all_tables(con: sqlite3.Connection, limit: Optional[int] = None):
    populate_tracks_table(con, limit)


def create_track_table(con: sqlite3.Connection):
    sql = """
        CREATE TABLE IF NOT EXISTS tracks (
            track_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            artist TEXT NOT NULL,
            spotify_preview_url TEXT NOT NULL,
            lastfm_url TEXT NOT NULL,
            spotify_id TEXT NOT NULL
        );
    """
    con.execute(sql)


def create_users_table(con: sqlite3.Connection):
    sql = """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            taste_profile_id TEXT NOT NULL
        );
    """
    con.execute(sql)


def create_tags_table(con: sqlite3.Connection, tags: List[str]):
    tags_sql = ", ".join(
        f"'{tag_name}' INTEGER NOT NULL" for tag_name in tags
    )
    sql = f"""
        CREATE TABLE IF NOT EXISTS tags (
            track_id INTEGER,
            {tags_sql},
            FOREIGN KEY (track_id) REFERENCES tracks (track_id)
        );
    """
    con.execute(sql)


def create_similar_table(con: sqlite3.Connection):
    sql = """
        CREATE TABLE IF NOT EXISTS similar (
            track_id_a INTEGER, track_id_b INTEGER, score REAL,
            PRIMARY KEY (track_id_a, track_id_b),
            FOREIGN KEY (track_id_a) REFERENCES tracks (track_id),
            FOREIGN KEY (track_id_b) REFERENCES tracks (track_id)
        );
    """
    con.execute(sql)


def create_track_user_table(con: sqlite3.Connection):
    sql = """
        CREATE TABLE IF NOT EXISTS track_users (
            track_id INTEGER, user_id INTEGER, playcount INTEGER,
            PRIMARY KEY (track_id, user_id),
            FOREIGN KEY (track_id) REFERENCES tracks (track_id),
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        );
    """
    con.execute(sql)




