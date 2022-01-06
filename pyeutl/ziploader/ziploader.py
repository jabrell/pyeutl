import pandas as pd
from .utils import (load_zipped_file, map_if_exists, get_mapper)
from .category_mappings import (map_activity_category, map_account_category,
                              map_nace_category)

def get_installations(fn_zip, 
                      drop=["latitudeEutl", "longitudeEutl", "nace15_id", "nace20_id", 
                            "created_on", "updated_on"]):
    """Load installation data from zip and add labels
    :param fn_zip: <string> name of zip file with data
    :param drop: <list: string> with column names to drop
    :return: <pd.DataFrame>"""
    # get installation and drop columns 
    df = load_zipped_file(fn_zip, "installation.csv")  
    cols = [c for c in df.columns if c not in drop]
    df = df[cols].copy()    
    
    # impose relations
    mapper = get_mapper(fn_zip, "activity_type.csv")
    df = map_if_exists(df, mapper, "activity_id", "activity", )
    df = map_if_exists(df, map_activity_category, "activity_id", "activityCategory", )
    mapper = get_mapper(fn_zip, "country_code.csv")
    df = map_if_exists(df, mapper, "registry_id", "registry")
    df = map_if_exists(df, mapper, "country_id", "country")
    mapper = get_mapper(fn_zip, "nace_code.csv")
    r_mapper = {} # ensure correct types between map and data frame
    for k, v in mapper.items():
        try:
            r_mapper[float(k)] = v
        except:
            pass
    df = map_if_exists(df, r_mapper, "nace_id", "nace")
    df = map_if_exists(df, map_nace_category, "nace_id", "naceCategory")
    return df


def get_compliance(fn_zip, df_installation=None, create_id=True, 
                   drop=["surrenderedCummulative", "surrenderedCummulative","created_on", "updated_on"]):
    """Load compliance data from zip and add labels and installation information
    :param fn_zip: <string> name of zip file with data
    :parma create_id: <boolean> True to create a column with a unique if combining installation
              id and year of compliance
    :param df_installations: <pd.DataFrame> with installation information to be included 
              index column has to be named "id"
    :param drop: <list: string> with column names to drop
    :return: <pd.DataFrame>"""
    df = load_zipped_file(fn_zip, "compliance.csv") 
    cols = [c for c in df.columns if c not in drop]
    df = df[cols].copy()
    # get compliance codes
    mapper = get_mapper(fn_zip, "compliance_code.csv")
    df = map_if_exists(df, mapper, "compliance_id", "complianceCode")
    # order of columns
    # create an unique id
    if create_id: 
        df["id"] = df.installation_id + "_" + df.year.map(str)
        df = df[["id"] + list(df.columns[:-1])]
    # add installation informations
    if df_installation is not None:
        df_installation = df_installation.rename(columns={"id": "installation_id"})
        df = df.merge(df_installation, on="installation_id", how="left")
        
    return df


def get_accounts(fn_zip, drop=["created_on", "updated_on"],
                df_installation=None, prefix_installation="installation"):
    """Load and aggregate account data from zip and add labels
    :param fn_zip: <string> name of zip file with data
    :param drop: <list: string> with column names to drop
    :param df_installations: <pd.DataFrame> with installation information to be included 
                  index column has to be named "id"s
    :param prefix_installation: <string> prefix for installation columns to avoid column
                  name conflicts. Only applied to columns with name conflict.
    :return: <pd.DataFrame>"""
    # get account and drop columns 
    df = load_zipped_file(fn_zip, "account.csv")  
    cols = [c for c in df.columns if c not in drop]
    df = df[cols].copy()    

    # impose relations
    mapper = get_mapper(fn_zip, "country_code.csv")
    df = map_if_exists(df, mapper, "registry_id", "registry")
    mapper = get_mapper(fn_zip, "account_type.csv")
    df = map_if_exists(df, mapper, "accountType_id", "accountType")
    df = map_if_exists(df, map_account_category, "accountType_id", "accountCategory")
    
    # merge installation information
    if df_installation is not None:
        cols_rename = {c: prefix_installation + c.capitalize()  
                       for c in df_installation.columns if c in df.columns and c != "id"}
        df_installation = df_installation.rename(columns=cols_rename) 
        df = df.merge(df_installation, left_on="installation_id", right_on="id", how="left",
                    suffixes=["", "_y"]).drop("id_y", axis=1)
    return df


def get_transactions(fn_zip, drop=[], freq="D",
                    df_account=None,
                    prefix_account={"transferring": "transferring", "acquiring": "acquiring"}):
    """Load and aggregate transaction data from zip and add labels
    :param fn_zip: <string> name of zip file with data
    :param drop: <list: string> with column names to drop
    :param freq: <string> frequency for resampling
    :param df_account: <pd.DataFrame> with account information
    :param prefix_account: <dict> with prefix for account columns for 
                transferring and acquiring accounts
    :return: <pd.DataFrame>"""
    # get transactions, drop columns and resample 
    df = load_zipped_file(fn_zip, "transaction.csv", {"parse_dates": ["date"]})  
    cols = [c for c in df.columns if c not in drop]
    df = df[cols].copy()
    groups = [pd.Grouper(key="date", freq=freq)] + [c for c in cols if c not in ["amount", "date"]]
    df = df.groupby(groups).amount.sum().reset_index()
    
    # get mappings for references and impose labels
    mapper = get_mapper(fn_zip, "transaction_type_main.csv")
    df = map_if_exists(df, mapper, "transactionTypeMain_id", "transactionTypeMain", )
    mapper = get_mapper(fn_zip, "transaction_type_supplementary.csv")
    df = map_if_exists(df, mapper, "transactionTypeSupplementary_id", "transactionTypeSupplementary", )
    mapper = get_mapper(fn_zip, "unit_type.csv")
    df = map_if_exists(df, mapper, "unitType_id", "unitType", )
    
    # impose account information
    rename_cols = {c: prefix_account["transferring"] + c[0].capitalize() + c[1:] for c in df_account.columns}
    rename_cols["id"] = "transferringAccount_id"
    df = df.merge(df_account.rename(columns=rename_cols), on="transferringAccount_id", how="left")

    rename_cols = {c: prefix_account["acquiring"] + c[0].capitalize() + c[1:] for c in df_account.columns}
    rename_cols["id"] = "acquiringAccount_id"
    df = df.merge(df_account.rename(columns=rename_cols), on="acquiringAccount_id", how="left")    
    
    return df
