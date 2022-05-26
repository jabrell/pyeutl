from getpass import getpass
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.inspection import inspect
from .model import Base
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from io import StringIO
import csv
import pandas as pd 
from zipfile import ZipFile
from .model import (TransactionTypeMain, TransactionTypeSupplementary,
                            Country, ComplianceCode, UnitType, AccountType,
                            ActivityType, NaceCode, OffsetProject, Installation,
                            Compliance, Surrender)
class DataAccessLayer:
    """Class managing database access"""

    def __init__(self, user, host, db, passw, port=5432,
                 echo=False, encoding="utf-8", connect=True,
                 base=None):
        """Constructor for data access class.
        Default access is to local database
        :param user: <string> user name
        :param host: <string> host address
        :param db: <string> database name
        :parm port: <int> port of database
        :param echo: <string, boolean> whther to echo sql statements, "debug" for verbose output
        :param encoding: <string> database encoding
        :param connect: <boolean> True to establish immediate connection to database
                        default: True
        :param base: <sqlalchemy.ext.declarative.declarative_base> Base class of ORM 
        """
        self.engine = None
        self.user = user
        self.host = host
        self.db = db
        if base is None: # no custum declarative base, so use the one provided by eutl orm
            self.Base = Base
        else:
            self.Base = base

        self.conn_string = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (user, passw, host, port, db)
        self.encoding = encoding
        self.echo = echo
        if connect:
            self.connect()            
        
    def connect(self):
        """ Connects to database """
        if self.engine is None:
            self.engine = create_engine(self.conn_string, echo=self.echo, encoding=self.encoding)
            self.Base.metadata.create_all(self.engine)
            self.metadata = MetaData(bind=self.engine)
            self.Session = sessionmaker(bind=self.engine)
            self.session = self.Session()

    def empty_database(self, askConfirmation=True):
        """ Deletes all tables from database connectd by engine 
        askConfirmation: <boolean> true to ask for typed confirmation"""
        self.metadata = MetaData(bind=self.engine)
        self.metadata.reflect()
        if len(self.engine.table_names()) > 0:
            if askConfirmation:
                confirm = getpass("Do really want to drop all tables? Enter Yes for confirmation: ")
            else:
                confirm = "yes"
            if confirm.lower() == "yes":
                for i, tbl in enumerate(reversed(self.metadata.sorted_tables)):
                    if tbl.name in ["spatial_ref_sys"]:
                        continue
                    print(i, tbl)
                    tbl.drop()
                print("Tables deleted")
            else:
                print("#### Tables still in database ####")
        self.metadata.reflect()
        self.Base.metadata.create_all(self.engine)
        
    def insert_df(self, df, obj, update=False,
                      bulk_insert=False, verbose=False):
        """Inserts dataframe to database using session and ORM object.
        Dataframe has to have columns matching fields of the OMR objects
        :param df: <pd.DataFrame> with data
        :param obj: <ORM object>
        :param update: <boolean> True to update existing rows
        :param bulk_insert: <boolean> True for buli insert of intems
        :parma verbose: <boolean> to print all keys not inserted
        """
        printed = False
        # get primary key names
        pk_names = [i.name for i in inspect(obj).primary_key]

        # replace Null values by None
        df_ = self._replace_null(df)
        to_add = []
        for item in df_.to_dict(orient="records"):
            # get primary key
            pk = {k: v for k, v in item.items() if k in pk_names}
            exists = False
            try:
                qry = self.session.query(obj).filter_by(**pk)
                exists = qry.count() > 0
            except ProgrammingError:
                pass
            if exists:  # with same key already in database
                if update:
                    qry.delete()
                else:
                    if verbose:
                        print("Did not insert", pk)
                    else:
                        if not printed:
                            print("Some entries not inserted due to key duplication.")
                            printed = True
                    continue
            obj_to_insert = obj(**item)
            if bulk_insert:
                to_add.append(obj_to_insert)
            else:
                self.session.add(obj_to_insert)
        if bulk_insert:
            self.session.add_all(to_add)
        self.session.commit()

    def insert_df_large(self, df, name, integerColumns=None,
                        schema=None, if_exists="fail", 
                        index=False, index_label=None, 
                        chunksize=1000000, dtype=None):
        """ Wrapper for pandas to_sql function using a more efficient insertion function.
        Likely only works under psycopg2 and postrgres
        :parm df: <pd.DataFrame> to be inserted
        :param name: <string> name if table
        :param integerColumns: <list: string> name of columns to be inserted as int
        :param schema: <string> pecify the schema (if database flavor supports this). If None, use default schema.
        :param chunksize: <int> number of rows to be inserted at once. In contrast to pandas implementeation
                    here we use explicit slicing of the dataframe.
        :param if_exists: {"fail", "replace", "append"}, default "fail", How to behave if the table already exists.
        :param index: <boolean> whetehr to also insert index
        :param index_label: <string> Column label for index column(s)
        :param dtype: <dict> or <scalar> Specifying the datatype for columns.
                    If a dictionary is used, the keys should be the column names and the values should be the SQLAlchemy types or strings for the sqlite3 legacy mode.
                    If a scalar is provided, it will be applied to all columns.
        """
        def psql_insert_copy(table, con, keys, data_iter):
            """ Execute SQL statement inserting data
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

                columns = ', '.join('"{}"'.format(k) for k in keys)
                if table.schema:
                    table_name = '{}.{}'.format(table.schema, table.name)
                else:
                    table_name = table.name

                sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                    table_name, columns)
                cur.copy_expert(sql=sql, file=s_buf)

        def df_to_list_of_chuncks(df, chunksize=100000):
            lst_df = [df[i: i + chunksize] for i in range(0, df.shape[0], chunksize)]
            return lst_df

        def prepare_int_cols_for_sql_insert(df, int_cols):
            """ For fast insertion into the database using the csv IO stream we need interegers to be properly formated.
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
            df_out.to_sql(name=name, con=self.session.get_bind(), 
                          if_exists=if_exists, schema=schema,
                          index=index, index_label=index_label,
                          dtype=dtype,
                          method=psql_insert_copy)
        
    @staticmethod
    def _replace_null(df):
        """replaces nan and nat in dataframe by None values for database insertion"""
        df_ = df.copy()
        dt_cols = df_.select_dtypes(include=['datetime64']).columns
        for c in dt_cols:   # convert datetimes to objects to replace nat
            df_[c] = df_[c].astype("object")
        df_ = df_.where(df.notnull(), None)
        return df_
        
    @staticmethod
    def prepare_int_cols_for_sql_insert(df, int_cols):
        """ For fast insertion into the database using the csv IO stream we need interegers to be properly formated.
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

    def create_database(self, fn_source):
        """Create Postres-Eutl database based in zipped eutl csv datafiles.
        Note that data already in the database will be deleted. 
        :param fn_source: <string> path to sour zip-file containing eutl data in csv format"""
        self.empty_database(askConfirmation=True)
        with ZipFile(fn_source, 'r') as fzip:
            # insert basic tables
            print("---- Insert lookup tables")
            self.insert_df(pd.read_csv(fzip.open("nace_code.csv")).sort_values("level"), NaceCode)
            self.insert_df(pd.read_csv(fzip.open("compliance_code.csv")), ComplianceCode)
            self.insert_df(pd.read_csv(fzip.open("country_code.csv"), keep_default_na=False), Country)
            self.insert_df(pd.read_csv(fzip.open("unit_type.csv")), UnitType)
            self.insert_df(pd.read_csv(fzip.open("activity_type.csv")), ActivityType)
            self.insert_df(pd.read_csv(fzip.open("account_type.csv")), AccountType)
            self.insert_df(pd.read_csv(fzip.open("transaction_type_supplementary.csv")), TransactionTypeSupplementary)
            self.insert_df(pd.read_csv(fzip.open("transaction_type_main.csv")), TransactionTypeMain) 
            # projects
            print("---- Insert offset projects")
            self.insert_df_large(
                pd.read_csv(fzip.open("project.csv")).drop(["created_on", "updated_on", "source"], axis=1), 
                "offset_project", integerColumns=["id", "track"], if_exists="append") 
            # Installations
            print("---- Insert installations")
            df = pd.read_csv(fzip.open("installation.csv"), 
                            dtype={"nace15_id": "str", "nace20_id": "str","nace_id": "str"}
                            ).drop(["created_on", "updated_on"], axis=1)
            self.insert_df_large(df, "installation", integerColumns=["euEntitlement", "chEntitlement"], if_exists="append")
            # Compliance
            print("---- Insert compliance data")
            df = pd.read_csv(fzip.open("compliance.csv")).drop(["created_on", "updated_on"], axis=1)
            int_cols = ['allocatedFree', 'allocatedNewEntrance', 'allocatedTotal', "allocated10c",
                        'verified', 'verifiedCummulative', 'verifiedUpdated', 'surrendered', 
                        'surrenderedCummulative']
            self.insert_df_large(df, "compliance", integerColumns=int_cols, if_exists="append")
            # Surrender
            print("---- Insert surrendering data")
            df = pd.read_csv(fzip.open("surrender.csv")).drop(["created_on", "updated_on"], axis=1)
            int_cols = ["amount", "project_id", "id"]
            self.insert_df_large(df, "surrender", integerColumns=int_cols, if_exists="append")   
            # insert account holders
            print("---- Insert account holders")
            df = pd.read_csv(fzip.open("account_holder.csv")).drop(["created_on", "updated_on"], axis=1)
            self.insert_df_large(df, "account_holder", if_exists="append") 
            # insert accounts
            print("---- Insert accounts")
            df = pd.read_csv(fzip.open("account.csv")).drop(["created_on", "updated_on"], axis=1)
            int_cols = ["id", "accountHolder_id"]   
            self.insert_df_large(df, "account", integerColumns=int_cols, if_exists="append")   
            # Transaction data
            print("---- Insert transactions")
            df = pd.read_csv(fzip.open("transaction.csv"))
            int_cols = ["id", "transactionTypeSupplementary_id", "transactionTypeMain_id",
                        "project_id", "amount", "transferringAccount_id", "acquiringAccount_id"]   
            self.insert_df_large(df, "transaction", integerColumns=int_cols, if_exists="append")                 
        return 