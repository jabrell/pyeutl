from pyeutl.orm import DataAccessLayer

if __name__ == "__main__":
    # database connection settings
    connectionSettings = dict(
        user="eutlAdmin", host="localhost", db="eutl_orm", passw="1234", port=5432
    )

    # path to zip-file with eutl data
    fn_source = "./eutl_2023.zip"

    dal = DataAccessLayer(**connectionSettings)
    dal.create_database(fn_source, askConfirmation=False)
