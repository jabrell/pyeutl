from pyeutl.orm import DataAccessLayer
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    connectionSettings = dict(
        user="postgres", host="localhost", db="eutl", passw="password", port=5432
    )
    dal = DataAccessLayer(**connectionSettings)
    dal.create_database()
    print("here")
