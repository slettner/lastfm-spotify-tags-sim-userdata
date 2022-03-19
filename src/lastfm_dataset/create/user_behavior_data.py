""" Implements access and utility for the Echo Nest Taste Profile data """
import json
import logging
import os
import sqlite3
from collections import defaultdict
from typing import Dict, Optional, Set, Tuple

from tqdm import tqdm

from lastfm_dataset.constants import (
    PATH_TO_SONG_ID2TRACK_ID_MAPPING,
    PATH_TO_TRAIN_TRIPLETS,
    PATH_TO_UNIQUE_TRACKS,
    PATH_TO_USER_TRACK_PAIR_COUNT,
)

log = logging.getLogger(__name__)


def populate_users_table(con: sqlite3.Connection):
    """
    Only adds users if they are associated with a song that exists in the tracks table.
    Also populates track_users table

    :param con: Database connection
    :return:
    """
    sql_insert_user = """
        INSERT INTO users(user_id)
        VALUES (?);
    """

    sql_insert_track_user = """
        INSERT INTO track_users(track_id, user_id, playcount)
        VALUES (?, ?, ?);
    """

    sql_get_all_track_ids = """
        SELECT track_id FROM tracks;
    """

    def _maybe_create_mapping() -> Dict:
        if os.path.isfile(PATH_TO_SONG_ID2TRACK_ID_MAPPING):
            log.info(
                f"Found existing mapping under {PATH_TO_SONG_ID2TRACK_ID_MAPPING}. Re-using."
            )
            with open(PATH_TO_SONG_ID2TRACK_ID_MAPPING, "r") as fh:
                return json.load(fh)
        else:
            log.info(
                f"Could not find existing mapping under {PATH_TO_SONG_ID2TRACK_ID_MAPPING}."
                f" Re-creating, takes some minutes."
            )
            new_map, _ = get_song_id2track_id_mapping()
            return new_map

    def _get_all_track_ids(connection: sqlite3.Connection) -> Set[str]:
        result = connection.execute(sql_get_all_track_ids).fetchall()
        return set([r["track_id"] for r in result])

    def _create_user(connection: sqlite3.Connection, _user_id: str) -> str:
        cur = connection.cursor()
        cur.execute(sql_insert_user, (_user_id,))
        connection.commit()
        return cur.lastrowid

    def _create_track_user_pair(
        connection: sqlite3.Connection, _track_id: str, _user_id: str, _play_count: int
    ) -> str:
        cur = connection.cursor()
        cur.execute(sql_insert_track_user, (_track_id, _user_id, _play_count))
        connection.commit()
        return cur.lastrowid

    def _save_summary(summary: Dict):
        with open(PATH_TO_USER_TRACK_PAIR_COUNT, "w") as fj:
            json.dump(summary, fj)

    song_id2track_id_mapping = _maybe_create_mapping()
    all_track_ids = _get_all_track_ids(con)
    added_users = set()
    user_songs_pair_count = defaultdict(int)

    log.info(
        "Checking all user song pairs for matches to the existing tracks and creating according records."
    )
    with open(PATH_TO_TRAIN_TRIPLETS, "r") as fh:
        lines = fh.readlines()
        for line in tqdm(lines, total=len(lines)):
            user_id, song_id, play_count = line.split("\t")
            if song_id in song_id2track_id_mapping:
                track_id = song_id2track_id_mapping[song_id]
                if track_id in all_track_ids:
                    if user_id not in added_users:
                        added_users.add(user_id)
                        _create_user(con, user_id)
                    _create_track_user_pair(con, track_id, user_id, play_count)
                    user_songs_pair_count[user_id] += 1

    _save_summary(user_songs_pair_count)
    log.info(
        f"Found {len(added_users)} unique users with at least one track associated in the tracks table."
    )


def get_song_id2track_id_mapping() -> Tuple[Dict, Dict]:
    result = dict()
    meta = {"duplicated_song_ids": []}
    with open(PATH_TO_UNIQUE_TRACKS, "r") as fh:
        lines = fh.readlines()
        for line in tqdm(lines, total=len(lines)):
            track_id, song_id, artist, track_name = line.split("<SEP>")
            if song_id in result:
                meta["duplicated_song_ids"].append([track_id, artist, track_name])
            else:
                result[song_id] = track_id
                meta[track_id] = [artist, track_name]
    log.info(f"Created mapping for {len(result)} songs.")
    return result, meta
