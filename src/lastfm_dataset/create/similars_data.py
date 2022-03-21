""" Implements access and utility for the raw LastFM data, specifically the similarity tags. """
import glob
import json
import logging
import os
import sqlite3
from typing import List, Set, Tuple

from tqdm import tqdm

from lastfm_dataset.constants import DATA_DIR, ROOT_DIR

log = logging.getLogger(__name__)


def populate_similars_table(con: sqlite3.Connection):
    """For all tracks in the tracks table, create records with (track_a, track_b)
    in case they are listed as similar by last fm"""
    sql_get_all_track_ids = """
        SELECT track_id FROM tracks;
    """

    sql_insert_similar = """
        INSERT OR IGNORE INTO similar(track_id_a, track_id_b, score)
        VALUES (?,?,?);
    """

    def _get_all_track_ids(connection: sqlite3.Connection) -> Set[str]:
        result = connection.execute(sql_get_all_track_ids).fetchall()
        return set([r["track_id"] for r in result])

    def _bulk_create_similar(
        connection: sqlite3.Connection, _data: List[Tuple[str, str, float]]
    ):
        cursor = connection.cursor()
        cursor.executemany(sql_insert_similar, _data)
        connection.commit()

    all_track_ids = _get_all_track_ids(con)
    all_tracks_json = list(
        glob.glob(os.path.join(ROOT_DIR, DATA_DIR, "**", "*.json"), recursive=True)
    )
    for path in tqdm(all_tracks_json):
        file_name = os.path.basename(path)
        track_id = os.path.splitext(file_name)[0]
        if track_id in all_track_ids:
            with open(path, "r") as fh:
                similars = json.load(fh)["similars"]
            data = [(track_id, sim[0], sim[1]) for sim in similars]
            _bulk_create_similar(con, data)
