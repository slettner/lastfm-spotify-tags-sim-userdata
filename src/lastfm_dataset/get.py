import sqlite3
from collections import defaultdict, namedtuple
from contextlib import contextmanager
from typing import Dict, List, Optional

from lastfm_dataset import DB_PATH, maybe_wrap_connection, row_factory
from lastfm_dataset.constants import PATH_TO_RESULT


@contextmanager
def get_connection(path: Optional[str] = None):
    if path is None:
        path = PATH_TO_RESULT

    con = sqlite3.connect(path, isolation_level=None)
    con.row_factory = row_factory

    try:
        yield con
    finally:
        con.close()


Track = namedtuple(
    "Tracks",
    ["track_id", "artist", "name", "spotify_preview_url", "lastfm_url", "spotify_id"],
)
User = namedtuple("Users", ["user_id"])


def _to_sql_string(vals: List[str]) -> str:
    ids = [f"'{_id}'" for _id in vals]
    return ", ".join(ids)


@maybe_wrap_connection
def get_tracks_by_ids(con: sqlite3.Connection, ids: List[str]) -> List[Track]:
    sql_query = f"""
        SELECT * FROM tracks WHERE track_id IN ({_to_sql_string(ids)});
    """
    rows = con.execute(sql_query).fetchall()
    return [Track(**row) for row in rows]


@maybe_wrap_connection
def get_tracks(
    con: sqlite3.Connection, offset: int = 0, limit: int = 100
) -> List[Track]:
    """Get tracks with pagination"""
    sql_query = f"""
        SELECT * FROM tracks LIMIT {limit} OFFSET {offset};
    """
    rows = con.execute(sql_query).fetchall()
    return [Track(**row) for row in rows]


@maybe_wrap_connection
def get_tags(con: sqlite3.Connection, tracks: List[Track]) -> Dict[str, List[str]]:
    """Keys are track_ids and values the corresponding tags"""
    sql_query = f"""
        SELECT * FROM tags WHERE tags.track_id in ({_to_sql_string([t.track_id for t in tracks])});
    """
    rows = con.execute(sql_query).fetchall()
    result = {
        row["track_id"]: [key for key, val in row.items() if val == 1] for row in rows
    }
    return result


@maybe_wrap_connection
def get_similars(con: sqlite3.Connection, tracks: List[Track]) -> Dict[str, List[str]]:
    """Keys are track_ids and values the corresponding similar track_ids. Values can be empty list. """
    sql_query = """
        SELECT * FROM similar WHERE similar.track_id_a = '{}';
    """
    result = dict()
    for track in tracks:
        rows = con.execute(sql_query.format(track.track_id)).fetchall()
        similars = [row["track_id_b"] for row in rows]
        result[track.track_id] = similars
    return result


@maybe_wrap_connection
def get_users(con: sqlite3.Connection, offset: int = 0, limit: int = 100) -> List[User]:
    """Get users with pagination."""
    sql_query = f"""
        SELECT * FROM users LIMIT {limit} OFFSET {offset};
    """
    rows = con.execute(sql_query)
    return [User(**row) for row in rows]


@maybe_wrap_connection
def get_track_listeners(
    con: sqlite3.Connection, tracks: List[Track]
) -> Dict[str, List[str]]:
    """Keys are track_ids and values are corresponding user that listened to that track"""
    sql_query = f"""
        SELECT * FROM track_users WHERE track_users.track_id in ({_to_sql_string([track.track_id for track in tracks])});
    """
    rows = con.execute(sql_query).fetchall()
    result = defaultdict(list)
    for row in rows:
        result[row["track_id"]].append(row["user_id"])
    return result


@maybe_wrap_connection
def get_user_listening_history(
    con: sqlite3.Connection, users: List[User]
) -> Dict[str, List[str]]:
    """Finds all tracks the users listened to. Every user has at least on track he listened to. """
    sql_query = f"""
        SELECT * FROM track_users WHERE track_users.user_id in ({_to_sql_string([u.user_id for u in users])});
    """
    rows = con.execute(sql_query).fetchall()
    result = defaultdict(list)
    for row in rows:
        result[row["user_id"]].append(row["track_id"])
    return result
