""" Implements access and utility for the existing, processed lastfm data with the Spotify Previews """
import glob
import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

from tqdm import tqdm

from lastfm_dataset.constants import (
    DATA_DIR,
    PATH_TO_NAME2ID_MAPPING,
    PATH_TO_PROCESSED_DB,
    ROOT_DIR,
)
from lastfm_dataset.create.utils import nsplit, row_factory

log = logging.getLogger(__name__)


def _decorate_with_limit(sql: str, limit: int) -> str:
    sql = sql.strip()[:-1]
    sql += f" LIMIT {limit};"
    return sql


@contextmanager
def processed_lastfm_database():
    con = sqlite3.connect(PATH_TO_PROCESSED_DB, isolation_level=None)
    con.row_factory = row_factory
    try:
        yield con
    finally:
        con.close()


def get_all_tags() -> List[str]:
    con = sqlite3.connect(PATH_TO_PROCESSED_DB)
    result = con.execute(
        """
        PRAGMA table_info(tags);
    """
    ).fetchall()
    result = [f"{item[1]}" for item in result]
    if "id_dataset" in result:
        result.remove("id_dataset")
    return result


def populate_tracks_table(con: sqlite3.Connection, limit: Optional[int] = None):
    all_tags = get_all_tags()
    sql_fetch = """
        SELECT * FROM metadata;
    """
    if limit is not None:
        sql_fetch = _decorate_with_limit(sql_fetch, limit)

    sql_insert_track = """
        INSERT INTO tracks(track_id, spotify_id, spotify_preview_url, lastfm_url, artist, name)
        VALUES (?, ?, ?, ?, ?, ?);
    """

    sql_insert_tags = f"""
        INSERT INTO tags('track_id', {", ".join([f"'{tag}'" for tag in all_tags])})
        VALUES ({",".join(['?'] * (len(all_tags) + 1))});
    """

    sql_get_tags = """
        SELECT * FROM tags WHERE id_dataset = '{}';
    """

    def _create_track(
        connection: sqlite3.Connection, track: Dict, name2track_id: Dict
    ) -> str:
        cur = connection.cursor()
        cur.execute(
            sql_insert_track,
            tuple(
                [
                    name2track_id[track["name"]],
                    track["id_spotify"],
                    track["url_spotify_preview"],
                    track["url_lastfm"],
                    track["artist"],
                    track["name"],
                ]
            ),
        )
        connection.commit()
        return name2track_id[track["name"]]

    def _create_tags(
        connection: sqlite3.Connection, tags_data: Dict, foreign_key: str
    ) -> str:
        cur = connection.cursor()
        all_data = tuple([foreign_key] + [tags_data[name] for name in all_tags])
        cur.execute(sql_insert_tags, all_data)
        connection.commit()
        return cur.lastrowid

    def _get_tags(connection: sqlite3.Connection, d_id: str) -> Dict:
        result = connection.execute(sql_get_tags.format(d_id)).fetchone()
        if len(result) == 0:
            raise RuntimeError(f"Could not find tags for dataset_id {d_id}")
        return result

    def _maybe_create_mapping() -> Dict:
        if os.path.isfile(PATH_TO_NAME2ID_MAPPING):
            log.info(
                f"Found existing mapping under {PATH_TO_NAME2ID_MAPPING}. Re-using."
            )
            with open(PATH_TO_NAME2ID_MAPPING, "r") as fh:
                return json.load(fh)
        else:
            log.info(
                f"Could not find existing mapping under {PATH_TO_NAME2ID_MAPPING}. Re-creating, takes some minutes."
            )
            new_map, _ = get_track_name2track_id_mapping()
            return new_map

    mapping = _maybe_create_mapping()
    with processed_lastfm_database() as con_processed:
        total_tracks_created = 0
        created_tracks_names = set()
        total_tracks_existing = con_processed.execute(
            """ SELECT count(*) FROM metadata;"""
        ).fetchone()["count(*)"]
        if limit is not None:
            total_tracks_existing = min(total_tracks_existing, limit)

        log.info("Creating tracks and tags table. Takes up to an hour.")
        with tqdm(total=total_tracks_existing) as pbar:
            for row in con_processed.execute(sql_fetch):
                if row["name"] in mapping and row["name"] not in created_tracks_names:
                    last_row_id = _create_track(con, row, mapping)
                    tags = _get_tags(con_processed, row["id_dataset"])
                    _create_tags(con, tags, foreign_key=last_row_id)
                    total_tracks_created += 1
                    created_tracks_names.add(row["name"])
                pbar.update(1)

    log.info(f"Created {total_tracks_created} tracks with according tags 🙌🏼.")


def get_track_name2track_id_mapping() -> Tuple[Dict, Dict]:
    def _get_all_song_names() -> List[str]:
        with processed_lastfm_database() as con_processed:
            names = con_processed.execute(
                """
                SELECT name FROM metadata;
            """
            ).fetchall()
            return [item["name"] for item in names]

    def _check_files(paths: List[str], result_dict: Dict, summary: Dict):
        for file_path in tqdm(paths):
            with open(file_path, "r") as fh:
                data = json.load(fh)
                song_name = data["title"]
                if song_name in all_song_names:
                    if song_name in result_dict:
                        summary["duplicate"].append(song_name)
                    else:
                        result_dict[song_name] = data["track_id"]
                else:
                    summary["not_found"].append(song_name)

    all_song_names = set(_get_all_song_names())
    result = {}
    summary = {"not_found": [], "duplicate": []}
    log.info(
        "Collecting all file path for matching track ids with existing songs. Take some seconds..."
    )
    all_tracks_json = list(
        glob.glob(os.path.join(ROOT_DIR, DATA_DIR, "**", "*.json"), recursive=True)
    )
    jobs = []
    split_sizes = []
    for split in nsplit(all_tracks_json, 10):
        split_sizes.append(str(len(split)))
        jobs.append(
            threading.Thread(target=_check_files, args=[split, result, summary])
        )
    log.info(
        f'Created 10 jobs which process splits of the following size {", ".join(split_sizes)}'
    )

    for job in jobs:
        job.start()

    for job in jobs:
        job.join()

    log.info(
        f"Collected track ids for {len(result)} of the original {len(all_song_names)} songs."
    )
    return result, summary
