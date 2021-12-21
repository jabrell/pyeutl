
import eutl_orm.model as mo
from eutl_orm import DataAccessLayer
from attic.connection_settings import connectionSettings

import datetime as dt
from sqlalchemy import or_
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np

dal = DataAccessLayer(**connectionSettings)
session = dal.session

# ------------------------------------------------------------
# Controls and constants
# ------------------------------------------------------------

do_data = False
do_plotting = False
year_range = range(2012, 2022)


# ------------------------------------------------------------
# Collect the data
# ------------------------------------------------------------

account_type_table = pd.read_sql(session.query(mo.AccountType).statement, con=session.bind)
account_type_table.rename(columns={'id': 'accountType_id'}, inplace=True)

if do_data:
    df = pd.DataFrame()
    df_account_type = pd.DataFrame()

    for year in year_range:

        account_query = session.query(mo.Account)\
            .filter(mo.Account.openingDate <= dt.date(year, 12, 31))\
            .filter(or_(mo.Account.closingDate >= dt.date(year, 1, 1),
                        mo.Account.isOpen))

        account_df = pd.read_sql(sql=account_query.statement,
                                 con=session.bind)

        n_account_holders = account_df.groupby('accountHolder_id').size().reset_index(name='counts')
        n_companies = account_df.groupby('companyRegistrationNumber').size().reset_index(name='counts')
        mah = account_df['accountHolder_id'].isna().sum()
        mcr = account_df['companyRegistrationNumber'].isna().sum()
        print(f'Missing values in {year}: {mah} (accountHolder) and {mcr} (company)')

        account_types = account_df.groupby('accountType_id').size().reset_index(name='counts')
        account_types = account_types.merge(account_type_table, on='accountType_id', how='left')

        df_account_type = df_account_type.append(pd.DataFrame([[year] + account_types['counts'].tolist()],
                                                 columns=['year'] + account_types['description'].tolist()))

        df = df.append(pd.DataFrame([[year, len(account_query.all()), len(n_account_holders), len(n_companies)]],
                                    columns=['year', 'nb_accounts', 'nb_accountHolders', 'nb_companies']))

    df.to_excel('../data/descriptive_stats_per_year.xlsx', index=False)
    df_account_type.fillna(value=0, inplace=True)
    df_account_type.to_excel('../data/descriptive_stats_per_year_accountTypes.xlsx', index=False)

else:
    df = pd.read_excel('../data/descriptive_stats_per_year.xlsx')
    df_account_type = pd.read_excel('../data/descriptive_stats_per_year_accountTypes.xlsx')

# ------------------------------------------------------------
# Do the plotting
# ------------------------------------------------------------

if do_plotting:
    # Plot number of accounts
    for v in ['accounts', 'accountHolders', 'companies']:
        fig, ax = plt.subplots(figsize=(10, 7))
        x = df['year']
        y = df[f'nb_{v}']
        plt.bar(x, y, color='navy')
        plt.title(f'Number of active {v}')
        ax.xaxis.set_ticks(year_range)
        plt.tight_layout()
        plt.savefig(f'../plots/descriptive/nb_{v}.png', dpi=500)
        plt.close()

    # Plot only relevant account types
    dfa_summary = pd.DataFrame(df_account_type.mean().reset_index())
    dfa_summary.rename(columns={'index': 'accountType', 0: 'count'}, inplace=True)
    dfa_summary = dfa_summary[dfa_summary['accountType'] != 'year']
    dfa_summary.sort_values(by=['count'], ascending=False, inplace=True)
    dfa_summary['plot'] = 0
    dfa_summary.loc[dfa_summary['count'] > 300, 'plot'] = 1
    account_types_to_be_plotted = dfa_summary[dfa_summary['plot'] == 1]['accountType'].tolist()

    col_list = list(df_account_type)
    for a in account_types_to_be_plotted:
        col_list.remove(a)
    df_account_type['other'] = df_account_type[col_list].sum(axis=1)
    account_types_to_be_plotted = account_types_to_be_plotted + ['other']

    dft = df_account_type[['year'] + account_types_to_be_plotted].copy()
    dft.to_excel('../data/descriptive_stats_per_year_accountTypes_PLOT.xlsx')

    # Stacked plot by
    cumval=0
    fig = plt.figure(figsize=(10, 7))
    for col in account_types_to_be_plotted:
        plt.bar(df_account_type['year'], df_account_type[col], bottom=cumval, label=col)
        cumval = cumval+df_account_type[col]
    _ = plt.xticks(rotation=30)
    _ = plt.legend(fontsize=10)
    plt.savefig(f'../plots/descriptive/nb_accountType.png', dpi=500)
    plt.close()

    # Plot number by account type
    for v in df_account_type.columns:
        if v != 'year':
            fig, ax = plt.subplots(figsize=(10, 7))
            x = df_account_type['year']
            y = df_account_type[v]
            plt.bar(x, y, color='navy')
            plt.title(f'Number of active {v}')
            ax.xaxis.set_ticks(year_range)
            plt.tight_layout()
            plt.savefig(f'../plots/descriptive/account_types/nb_{v}.png', dpi=500)
            plt.close()

# ----------------------------------------------------------------------
# Understand where accountHolder_id and company_id don't match
# ----------------------------------------------------------------------

account_query = session.query(mo.Account)\
    .filter(mo.Account.openingDate <= dt.date(2020, 12, 31))\
    .filter(or_(mo.Account.closingDate >= dt.date(2020, 1, 1),
                mo.Account.isOpen))

account_df = pd.read_sql(sql=account_query.statement, con=session.bind)\
    .fillna(value={'companyRegistrationNumber': 'NA'})

missing_companies = account_df[account_df.companyRegistrationNumber == 'NA']
missing_companies = missing_companies.merge(account_type_table, on='accountType_id', how='left', indicator=True)
if len(missing_companies[missing_companies._merge != 'both']) > 0:
    ValueError('Some not merged')
missing_companies.drop(labels='_merge', axis=1, inplace=True)

missing_companies.to_excel('../data/missing_companies_2021.xlsx')

df = account_df.groupby(['accountType_id', 'companyRegistrationNumber'], as_index=False)\
    ['id'].count()
df = df[df.id > 1]
df['double_ah'] = df['accountType_id'].value_counts()
df['double_cr'] = df['companyRegistrationNumber'].value_counts()
print(df)
