"""This script sets up the database from the data contained in data/eutl.zip.

To connect to the database, create a file attic/connection_settings.py containing following dictionary:

# attic/connection_settings.py
connectionSettings = dict(
    user="<USER_NAME>",
    host="localhost",
    db=<DB_NAME>,
    passw="<PASSWORD>",
    port=5432
)
"""
from eutl_orm import DataAccessLayer
from attic import paths
from attic.connection_settings import connectionSettings

# path to zip-file containing EUTL data
path_to_source_data = paths.path_data / "eutl.zip"

# connect to database
dal = DataAccessLayer(**connectionSettings)

# create database - this can last several minutes
dal.create_database(path_to_source_data)