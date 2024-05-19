import os
import csv
import logging
from typing import Any
from zipfile import ZipFile
from getpass import getpass
from io import StringIO
import pandas as pd
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.inspection import inspect
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from pyeutl.utils import download_data
from .model import (
    Base,
    TransactionTypeMain,
    TransactionTypeSupplementary,
    Country,
    ComplianceCode,
    UnitType,
    AccountType,
    ActivityType,
    NaceCode,
    TradingSystemCode,
)


class DataAccessLayer:
    """Class managing database access"""

    @property
    def session(self) -> Any:
        """Get a database session"""
        return self.Session()

    def __init__(
        self,
        user: str,
        host: str,
        db: str,
        passw: str,
        port: int = 5432,
        echo: bool | str = False,
        encoding: str = "utf-8",
        connect: bool = True,
        base: Any | None = None,
    ):
        """Constructor for data access class.
        Default access is to local database

        Args:
            user: <string> user name
            host: <string> host address
            db: <string> database name
            port: <int> port of database
            echo: <string, boolean> whether to echo sql statements,
                "debug" for verbose output
            encoding: <string> database encoding
            connect: <boolean> True to establish immediate connection to database
                default: True
            base: <sqlalchemy.ext.declarative.declarative_base> Base class of ORM
        """
        self.engine = None
        self.user = user
        self.host = host
        self.db = db
        if (
            base is None
        ):  # no custom declarative base, so use the one provided by eutl orm
            self.Base = Base
        else:
            self.Base = base

        self.conn_string = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            user,
            passw,
            host,
            port,
            db,
        )
        self.encoding = encoding
        self.echo = echo
        if connect:
            self.connect()

    def connect(self):
        """Connects to database"""
        if self.engine is None:
            self.engine = create_engine(
                self.conn_string, echo=self.echo, client_encoding=self.encoding
            )
            if not database_exists(self.engine.url):
                create_database(self.engine.url)
                print(f"Created new database '{self.db}'")
            self.Base.metadata.create_all(self.engine)
            self.metadata = MetaData()
            self.Session = sessionmaker(bind=self.engine)
            # self.session = self.Session()

    def empty_database(self, askConfirmation: bool = True):
        """Deletes all tables from database connected by engine

        Args:
            askConfirmation: <boolean> true to ask for typed confirmation
        """
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        existing_tables = self.metadata.sorted_tables
        if len(existing_tables) > 0:
            if askConfirmation:
                confirm = getpass(
                    "Do really want to drop all tables? Enter Yes for confirmation: "
                )
            else:
                confirm = "yes"
            if confirm.lower() == "yes":
                for i, tbl in enumerate(reversed(existing_tables)):
                    if tbl.name in ["spatial_ref_sys"]:
                        continue
                    print(i, tbl)
                    tbl.drop(bind=self.engine)
                print("Tables deleted")
            else:
                print("#### Tables still in database ####")
        self.metadata.reflect(bind=self.engine)
        self.Base.metadata.create_all(self.engine)

    def insert_df(
        self,
        df: pd.DataFrame,
        obj: Any,
        update: bool = False,
        bulk_insert: bool = False,
        verbose: bool = False,
    ) -> None:
        """Inserts dataframe to database using session and ORM object.
        Dataframe has to have columns matching fields of the OMR objects
            df: <pd.DataFrame> with data
            obj: <ORM object>
            update: <boolean> True to update existing rows
            bulk_insert: <boolean> True for bulk insert of items
            verbose: <boolean> to print all keys not inserted
        """
        printed = False
        # get primary key names
        pk_names = [i.name for i in inspect(obj).primary_key]

        # replace Null values by None
        df_ = self._replace_null(df)
        to_add = []
        with self.Session() as session:
            for item in df_.to_dict(orient="records"):
                # get primary key
                pk = {k: v for k, v in item.items() if k in pk_names}
                exists = False
                try:
                    qry = session.query(obj).filter_by(**pk)
                    exists = qry.count() > 0
                except ProgrammingError:
                    pass
                if exists:  # with same key already in database
                    if update:
                        qry.delete()
                    else:
                        if verbose:
                            print(
                                f"Did not insert {pk} into table {obj.__tablename__}",
                            )
                        else:
                            if not printed:
                                print(
                                    f"Some entries not inserted into {obj.__tablename__} due to key duplication."
                                )
                                printed = True
                        continue
                obj_to_insert = obj(**item)
                if bulk_insert:
                    to_add.append(obj_to_insert)
                else:
                    session.add(obj_to_insert)
            if bulk_insert:
                session.add_all(to_add)
            session.commit()
        return

    def insert_df_large(
        self,
        df: pd.DataFrame,
        name: str,
        integerColumns: list[str] | None = None,
        schema: str | None = None,
        if_exists: str = "fail",
        index: bool = False,
        index_label: str | None = None,
        chunksize: int = 1000000,
        dtype: str | dict | None = None,
    ) -> None:
        """Wrapper for pandas to_sql function using a more efficient insertion function.
        Likely only works under psycopg2 and postrgres

        Args:
            df: <pd.DataFrame> to be inserted
            name: <string> name if table
            integerColumns: <list: string> name of columns to be inserted as int
            schema: <string> specify the schema (if database flavor supports this).
                If None, use default schema.
            chunksize: <int> number of rows to be inserted at once. In contrast to pandas
                    implementation here we use explicit slicing of the dataframe.
            if_exists: {"fail", "replace", "append"}, default "fail".
                How to behave if the table already exists.
            index: <boolean> whether to also insert index
            index_label: <string> Column label for index column(s)
            dtype: <dict> or <scalar> Specifying the datatype for columns.
                    If a dictionary is used, the keys should be the column names
                    and the values should be the SQLAlchemy types or strings for
                    the sqlite3 legacy mode.
                    If a scalar is provided, it will be applied to all columns.
        """

        def psql_insert_copy(table, con, keys, data_iter):
            """Execute SQL statement inserting data
            :param table : pandas.io.sql.SQLTable
            :parm con : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
            :param keys : list of str Column names
            :param data_iter : Iterable that iterates the values to be inserted"""
            # gets a DBAPI connection that can provide a cursor
            dbapi_con = con.connection
            with dbapi_con.cursor() as cur:
                s_buf = StringIO()
                writer = csv.writer(s_buf)
                writer.writerows(data_iter)
                s_buf.seek(0)

                columns = ", ".join('"{}"'.format(k) for k in keys)
                if table.schema:
                    table_name = "{}.{}".format(table.schema, table.name)
                else:
                    table_name = table.name

                sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
                cur.copy_expert(sql=sql, file=s_buf)

        def df_to_list_of_chuncks(df, chunksize=100000):
            lst_df = [df[i : i + chunksize] for i in range(0, df.shape[0], chunksize)]
            return lst_df

        def prepare_int_cols_for_sql_insert(df, int_cols):
            """For fast insertion into the database using the csv IO stream we need interegers to be properly formated.
            However, as long as we have Null values in the column, the column is of type float and integers are foramtted with
            .0 at the end. Thus we cast these integers to string with correct format.
            :param df: <pd.DataFrame>
            :param int_cols: <list:string> names of columns to be converted
            """

            def int_to_string(x):
                try:
                    return "%d" % x
                except (TypeError, ValueError):
                    return x

            for c in int_cols:
                df[c] = df[c].map(int_to_string)
            return df

        # convert int columns
        if integerColumns is not None:
            df_ = prepare_int_cols_for_sql_insert(df, int_cols=integerColumns)
        else:
            df_ = df.copy()

        # create chunks of dataframe and slice over it
        lst_df = df_to_list_of_chuncks(df_, chunksize=chunksize)
        for i, df_out in enumerate(lst_df):
            if (i % 10 == 0) and (i > 0):
                print("#### Commit chunck %d of %d" % ((i + 1), len(lst_df)))
            # insert data
            df_out.to_sql(
                name=name,
                con=self.Session().get_bind(),
                if_exists=if_exists,
                schema=schema,
                index=index,
                index_label=index_label,
                dtype=dtype,
                method=psql_insert_copy,
            )
        return

    @staticmethod
    def _replace_null(df: pd.DataFrame) -> pd.DataFrame:
        """replaces nan and nat in dataframe by None values for database insertion"""
        df_ = df.copy()
        dt_cols = df_.select_dtypes(include=["datetime64"]).columns
        for c in dt_cols:  # convert datetimes to objects to replace nat
            df_[c] = df_[c].astype("object")
        df_ = df_.where(df.notnull(), None)
        return df_

    @staticmethod
    def _prepare_int_cols_for_sql_insert(
        df: pd.DataFrame, int_cols: list[str]
    ) -> pd.DataFrame:
        """For fast insertion into the database using the csv IO stream we need
            integers to be properly formatted.
            However, as long as we have Null values in the column, the column is
            of type float and integers are formatted with
            .0 at the end. Thus we cast these integers to string with correct format.

        Args:
            df (pd.DataFrame): input dataframe
            int_cols (list[str]): names of columns to be converted

        Returns:
            pd.DataFrame: dataframe with integer columns formatted
        """

        def int_to_string(x):
            try:
                return "%d" % x
            except (TypeError, ValueError):
                return x

        for c in int_cols:
            df[c] = df[c].map(int_to_string)
        return df

    def create_database(
        self, fn_source: str | None = None, askConfirmation: bool = True
    ) -> None:
        """Create Postres-Eutl database based in zipped eutl csv datafiles.
        Note that data already in the database will be deleted.

        Args:
            fn_source (str): path to zip file with eutl data. If none, data will be
                downloaded from euets.info for the most recent year of the database.
            askConfirmation (bool, optional): True to ask for confirmation. Defaults to True.
        """
        delete_input = False
        if fn_source is None:
            print("No source file provided. Download data from euets.info")
            delete_input = True
            fn_source = download_data()

        # empty the database
        self.empty_database(askConfirmation=askConfirmation)
        with ZipFile(fn_source, "r") as fzip:
            # insert basic tables
            print("---- Insert lookup tables")
            self.insert_df(
                pd.read_csv(fzip.open("nace_code.csv")).sort_values("level"), NaceCode
            )
            self.insert_df(
                pd.read_csv(fzip.open("compliance_code.csv")), ComplianceCode
            )
            self.insert_df(
                pd.read_csv(fzip.open("country_code.csv"), keep_default_na=False),
                Country,
            )
            self.insert_df(pd.read_csv(fzip.open("unit_type.csv")), UnitType)
            self.insert_df(pd.read_csv(fzip.open("activity_type.csv")), ActivityType)
            self.insert_df(pd.read_csv(fzip.open("account_type.csv")), AccountType)
            self.insert_df(
                pd.read_csv(fzip.open("transaction_type_supplementary.csv")),
                TransactionTypeSupplementary,
            )
            self.insert_df(
                pd.read_csv(fzip.open("transaction_type_main.csv")), TransactionTypeMain
            )
            self.insert_df(
                pd.read_csv(fzip.open("trading_system_code.csv")), TradingSystemCode
            )
            # projects
            print("---- Insert offset projects")
            self.insert_df_large(
                pd.read_csv(fzip.open("project.csv")).drop(
                    ["created_on", "updated_on", "source"], axis=1
                ),
                "offset_project",
                integerColumns=["id", "track"],
                if_exists="append",
            )
            # Installations
            print("---- Insert installations")
            df = pd.read_csv(
                fzip.open("installation.csv"),
                dtype={"nace15_id": "str", "nace20_id": "str", "nace_id": "str"},
                low_memory=False,
            ).drop(["created_on", "updated_on"], axis=1)
            self.insert_df_large(
                df,
                "installation",
                integerColumns=["euEntitlement", "chEntitlement"],
                if_exists="append",
            )
            # Compliance
            print("---- Insert compliance data")
            df = pd.read_csv(fzip.open("compliance.csv"), low_memory=False).drop(
                ["created_on", "updated_on"], axis=1
            )
            int_cols = [
                "allocatedFree",
                "allocatedNewEntrance",
                "allocatedTotal",
                "allocated10c",
                "verified",
                "verifiedCummulative",
                "verifiedUpdated",
                "surrendered",
                "surrenderedCummulative",
                "balance",
                "penalty",
            ]
            self.insert_df_large(
                df, "compliance", integerColumns=int_cols, if_exists="append"
            )
            # Surrender
            print("---- Insert surrendering data")
            df = pd.read_csv(fzip.open("surrender.csv")).drop(
                ["created_on", "updated_on"], axis=1
            )
            int_cols = ["amount", "project_id", "id"]
            self.insert_df_large(
                df, "surrender", integerColumns=int_cols, if_exists="append"
            )
            # insert account holders
            print("---- Insert account holders")
            df = pd.read_csv(fzip.open("account_holder.csv")).drop(
                ["created_on", "updated_on"], axis=1
            )
            self.insert_df_large(df, "account_holder", if_exists="append")
            # insert accounts
            print("---- Insert accounts")
            df = pd.read_csv(fzip.open("account.csv"), low_memory=False).drop(
                ["created_on", "updated_on"], axis=1
            )
            int_cols = ["id", "accountHolder_id", "yearValid"]
            self.insert_df_large(
                df, "account", integerColumns=int_cols, if_exists="append"
            )
            # Transaction data
            print("---- Insert transactions")
            df = pd.read_csv(fzip.open("transaction.csv"))
            int_cols = [
                "id",
                "transactionTypeSupplementary_id",
                "transactionTypeMain_id",
                "project_id",
                "amount",
                "transferringAccount_id",
                "acquiringAccount_id",
                "acquiringYear",
                "transferringYear",
            ]
            self.insert_df_large(
                df, "transaction", integerColumns=int_cols, if_exists="append"
            )

        # delete the downloaded source file
        if delete_input:
            os.remove(fn_source)
        return
