import pandas as pd
from zipfile import ZipFile

def load_zipped_file(fn_zip, fn_file, read_csv_args={}):
    """Load file in zip archiv
    :param fn_zip: <string> name of zip file
    :param fn_file: <string> name of file in zip file
    :param read_csv_args: <dict> passed to pandas read_csv"""
    zip_file = ZipFile(fn_zip)
    df = pd.read_csv(zip_file.open(fn_file), **read_csv_args)
    return df


def get_mapper(fn_zip, fn_file, key="id", value="description"):
    """Get mapping dictionary from zip file
    :param fn_zip: <string> name of zip file
    :param fn_file: <string> name of file in zip file
    :param key: <string> name of key column
    :param value: <string> name of value column
    :param: <dict: key -> value>
    """
    zip_file = ZipFile(fn_zip)
    df = pd.read_csv(zip_file.open(fn_file))
    return dict(zip(df[key],df[value]))


def map_if_exists(df, mapper, col, col_mapped, default=None, drop_col=False):
    """ Applies mapper dictionary on values in col and creates new column 
        with mapped values
    param df: <pd.DataFrame> 
    param mapper: <dict> with mapping imposed 
    param col: <string> name of column for mapping
    param col_mapped: <string> name of column for mapping
    default: default value for mapping
    drop_col: <boolean> true to drop column with original values
    """
    if col not in df.columns:
        return df
    df[col_mapped] = df[col].map(lambda x: mapper.get(x, default))
    if drop_col:
        df = df.drop(col, axis=1)
    return df

