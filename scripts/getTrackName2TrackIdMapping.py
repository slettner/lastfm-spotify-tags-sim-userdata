"""
This script create a dictionary where track names map to track ids from the MSD dataset.
The process is very IO bound because we have to open 1m json files to check for the track name.
Threading to the rescue, but still takes a couple of minutes.
"""
import json
import logging

from lastfm_dataset.constants import PATH_TO_NAME2ID_MAPPING, PATH_TO_NAME2ID_SUMMARY
from lastfm_dataset.create.track_and_tags_data import get_track_name2track_id_mapping

log = logging.getLogger(__name__)


def main():
    result, summary = get_track_name2track_id_mapping()

    with open(PATH_TO_NAME2ID_MAPPING, "w") as fj:
        json.dump(result, fj)
    with open(PATH_TO_NAME2ID_SUMMARY, "w") as fj:
        json.dump(summary, fj)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
