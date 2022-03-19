"""
This script create a dictionary where track names map to track ids from the MSD dataset.
The process is very IO bound because we have to open 1m json files to check for the track name.
Threading to the rescue, but still takes a couple of minutes.
"""
import json
import os
import logging

from lastfm_dataset.create.processed_lastfm_data import get_track_name2track_id_mapping
from lastfm_dataset.constants import ROOT_DIR, DATA_DIR

log = logging.getLogger(__name__)


def main():
    result, summary = get_track_name2track_id_mapping()

    save_path_result = os.path.join(ROOT_DIR, DATA_DIR, 'name2id_mapping.json')
    save_path_summary = os.path.join(ROOT_DIR, DATA_DIR, 'summary.json')
    with open(save_path_result, 'w') as fj:
        json.dump(result, fj)
    with open(save_path_summary, 'w') as fj:
        json.dump(summary, fj)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
