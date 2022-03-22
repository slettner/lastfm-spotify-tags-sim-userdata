import os
from pathlib import Path

ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent
DATA_DIR = "data"

PATH_TO_RESULT = os.path.join(ROOT_DIR, DATA_DIR, "dataset.db")
PATH_TO_PROCESSED_DB = os.path.join(ROOT_DIR, DATA_DIR, "lastfm_dataset_2020.db")
PATH_TO_TRAIN_TRIPLETS = os.path.join(ROOT_DIR, DATA_DIR, "train_triplets.txt")
PATH_TO_NAME2ID_MAPPING = os.path.join(ROOT_DIR, DATA_DIR, "name2track_id_mapping.json")
PATH_TO_NAME2ID_SUMMARY = os.path.join(ROOT_DIR, DATA_DIR, "name2track_id_summary.json")
PATH_TO_UNIQUE_TRACKS = os.path.join(ROOT_DIR, DATA_DIR, "unique_tracks.txt")
PATH_TO_SONG_ID2TRACK_ID_MAPPING = os.path.join(
    ROOT_DIR, DATA_DIR, "song_id2track_id_mapping.json"
)
PATH_TO_SONG_ID2TRACK_ID_SUMMARY = os.path.join(
    ROOT_DIR, DATA_DIR, "song_id2track_id_summary.json"
)
PATH_TO_USER_TRACK_PAIR_COUNT = os.path.join(
    ROOT_DIR, DATA_DIR, "user_track_pair_count.json"
)

TOTAL_TRACKS = 48056
TOTAL_USERS = 954382
NUM_TAGS = 100
TOTAL_TRACK_USER_PAIRS = 9087173
TOTAL_TRACK_SIMILAR_PAIRS = 4651751
