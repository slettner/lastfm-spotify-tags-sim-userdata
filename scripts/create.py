""" This scripts create the entire dataset from scratch. """

import logging

from lastfm_dataset.create.base_data import (
    create_all_tables,
    create_database_file,
    populate_all_tables,
)

log = logging.getLogger(__name__)


def main():
    log.info("Creating Tables.")
    with create_database_file(overwrite_existing=True) as con:
        create_all_tables(con)
        populate_all_tables(con, limit=100)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
