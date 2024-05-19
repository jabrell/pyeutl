from pyeutl.orm import DataAccessLayer
from pyeutl.ziploader import get_transactions
import logging

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    connectionSettings = dict(
        user="eutlAdmin", host="localhost", db="eutl_orm", passw="1234", port=5432
    )

    connectionSettings = dict(
        user="postgres", host="localhost", db="eutl", passw="password", port=5432
    )

    dal = DataAccessLayer(**connectionSettings)
    dal.create_database()
    print("here")
