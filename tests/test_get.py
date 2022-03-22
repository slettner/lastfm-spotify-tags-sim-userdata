import sqlite3
from typing import List

import pytest

from lastfm_dataset.get import (
    Track,
    User,
    get_connection,
    get_similars,
    get_tags,
    get_track_listeners,
    get_tracks,
    get_tracks_by_ids,
    get_user_listening_history,
    get_users,
    get_users_by_ids,
)


@pytest.fixture()
def con() -> sqlite3.Connection:
    with get_connection() as con:
        yield con


def is_valid_track_id(track_id: str) -> bool:
    if track_id.upper() != track_id:
        return False
    if not track_id.isalnum():
        return False
    if len(track_id) != 18:
        return False
    return True


def is_user_id(user_id: str) -> bool:
    if user_id.lower() != user_id:
        return False
    if not user_id.isalnum():
        return False
    if len(user_id) != 40:
        return False
    return True


def validate_user_ids_many(user_ids: List[str]) -> bool:
    return all([is_user_id(_id) for _id in user_ids])


def validate_track_ids_many(track_ids: List[str]) -> bool:
    return all([is_valid_track_id(_id) for _id in track_ids])


def extract_track_ids(tracks: List[Track]) -> List[str]:
    return [row.track_id for row in tracks]


def extract_user_ids(users: List[User]) -> List[str]:
    return [row.user_id for row in users]


def test_get_tracks(con: sqlite3.Connection):
    tracks_all = get_tracks(con, offset=0, limit=100)
    tracks_fist_half = get_tracks(con, offset=0, limit=50)
    tracks_second_half = get_tracks(con, offset=50, limit=50)

    assert len(tracks_all) == 100
    assert len(tracks_fist_half) == 50
    assert len(tracks_second_half) == 50

    assert extract_track_ids(tracks_fist_half) + extract_track_ids(
        tracks_second_half
    ) == extract_track_ids(tracks_all)
    assert validate_track_ids_many(extract_track_ids(tracks_all))


def test_get_tracks_by_ids(con: sqlite3.Connection):
    ids = ["TRLRGVX128E078EC1B", "TRXHDTA128F42A077A", "TRNZIAP128F93437BF"]
    tracks = get_tracks_by_ids(con, ids=ids)

    assert len(tracks) == 3
    assert sorted(ids) == sorted(extract_track_ids(tracks))
    assert validate_track_ids_many(extract_track_ids(tracks))


def test_get_tags(con: sqlite3.Connection):
    tracks = get_tracks(con, offset=0, limit=2)

    tags = get_tags(con, tracks)

    assert sorted(extract_track_ids(tracks)) == sorted(tags.keys())
    assert all([len(t) > 0 for t in tags.values()])


def test_get_similar(con: sqlite3.Connection):
    tracks = get_tracks(con, offset=0, limit=5)

    similar = get_similars(con, tracks)

    assert sorted(extract_track_ids(tracks)) == sorted(similar.keys())
    # assert all([len(s) for s in similar.values()])  # there can be tracks with no similar songs.
    assert validate_track_ids_many(list(similar.keys()))
    assert validate_track_ids_many([_id for sim in similar.values() for _id in sim])


def test_get_user(con: sqlite3.Connection):
    users_all = get_users(con, offset=0, limit=100)
    users_fist_half = get_users(con, offset=0, limit=50)
    users_second_half = get_users(con, offset=50, limit=50)

    assert len(users_all) == 100
    assert len(users_fist_half) == 50
    assert len(users_second_half) == 50

    assert extract_user_ids(users_all) == extract_user_ids(
        users_fist_half
    ) + extract_user_ids(users_second_half)
    assert validate_user_ids_many(extract_user_ids(users_all))


def test_get_users_by_ids(con: sqlite3.Connection):
    ids = [
        "b80344d063b5ccb3212f76538f3d9e43d87dca9e",
        "85c1f87fea955d09b4bec2e36aee110927aedf9a",
        "bd4c6e843f00bd476847fb75c47b4fb430a06856",
    ]
    users = get_users_by_ids(con, ids=ids)

    assert len(users) == 3
    assert sorted(ids) == sorted(extract_user_ids(users))
    assert validate_user_ids_many(extract_user_ids(users))


def test_get_track_listener(con: sqlite3.Connection):
    tracks = get_tracks(con, offset=0, limit=20)

    listeners = get_track_listeners(con, tracks)

    # not all tracks have a user that listened to them.
    assert set(extract_track_ids(tracks)) >= set(listeners.keys())

    assert validate_track_ids_many(list(listeners.keys()))
    assert validate_user_ids_many(
        [_id for users in listeners.values() for _id in users]
    )


def test_get_user_listening_history(con: sqlite3.Connection):
    users = get_users(con, offset=0, limit=2)

    history = get_user_listening_history(con, users)

    assert sorted(extract_user_ids(users)) == sorted(history.keys())
    assert all([len(s) > 0 for s in history.values()])
    assert validate_user_ids_many(list(history.keys()))
    assert validate_track_ids_many(
        [_id for tracks in history.values() for _id in tracks]
    )
