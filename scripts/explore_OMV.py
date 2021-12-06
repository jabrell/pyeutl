import sys
import os
from os.path import dirname
sys.path.append(dirname(__file__))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
import seaborn as sns
import datetime as dt

from eutl_orm import DataAccessLayer
from eutl_orm import Installation, Account
from attic import paths
from attic.connection_settings import connectionSettings

plt.rc('xtick', labelsize=14)  #fontsize of the x tick labels
plt.rc('ytick', labelsize=14)  #fontsize of the y tick labels
plt.rc('axes', titlesize=18)  #fontsize of the title
plt.rc('figure', titlesize=20)  #fontsize of the title
plt.rc('axes', labelsize=15)  #fontsize of the axes labels
plt.rc('legend', fontsize=15)  #fontsize of the axes labels
plt.xticks(rotation=45)

dal = DataAccessLayer(**connectionSettings)
session = dal.session

# ---------------------------------------------------------------------------------------------------
# Choose which installation to start with
# ---------------------------------------------------------------------------------------------------

installation_id = 'AT_219'
installation = session.query(Installation).get(installation_id)
print(f'Picked installation: {installation.name}')

compliance_data = installation.get_compliance()
compliance_data = compliance_data[compliance_data.year < 2028]

# Plot the compliance data
fig, ax = plt.subplots()
x = compliance_data['year']
compliance_data[['year', 'allocatedTotal', 'verified', 'surrendered']]\
    .set_index('year').div(1000000)\
    .plot(kind='bar', figsize=(10, 7), ylabel=r'Million tCO$_2$', grid=True)
plt.tight_layout()
plt.savefig('../plots/compliance.png')
plt.close()

# ---------------------------------------------------------------------------------------------------
# Move to account holder
# ---------------------------------------------------------------------------------------------------

operator_holding_account = installation.accounts[0]
print(f'Found {len(installation.accounts)} account holders')
account_holder = operator_holding_account.accountHolder
account_holder_id = operator_holding_account.accountHolder.id
print(f'Account Holder: {account_holder.name} with id {account_holder_id}')

# get all accounts linked to that accountholder
accounts = session.query(Account).filter(Account.accountHolder_id == account_holder_id).all()
acc_ids = [a.id for a in accounts]

# ---------------------------------------------------------------------------------------------------
# Get all transactions
# ---------------------------------------------------------------------------------------------------

transaction_tables = []
for account in accounts:
    transaction_tables.append(account.get_transactions())

transactions = pd.concat(transaction_tables)

# add date and datetime information to table (to price transactions with daily ETS price)
transactions["date"] = pd.to_datetime(transactions.index.date)
transactions["datetime"] = pd.to_datetime(transactions.index)

# aggregate transaction blocks into individual transactions
aggregated_transactions = transactions.groupby("transactionID").agg({"amount": "sum", "amount_directed": "sum",
                                                                     "transferringAccount_id": "first",
                                                                     "acquiringAccount_id": "first",
                                                                     "date": "first",
                                                                     "datetime": "first",
                                                                     "acquiringAccountName": "first",
                                                                     "transferringAccountName": "first",
                                                                     "transactionTypeMain": "first",
                                                                     "transactionTypeSupplementary": "first"})

# map all Accounts to their Account Holder
#print('Getting list of account and accountHolder IDs')
account_to_account_holder = pd.DataFrame(session.query(Account.id, Account.accountHolder_id).all())
n_accounts = len(account_to_account_holder)
#print(n_accounts, 'found')
account_to_account_holder.columns = ['id', 'accountHolder_id']
account_to_account_holder = account_to_account_holder.dropna()  # L: why are there holes in this table?
#print(n_accounts - len(account_to_account_holder), 'NAs dropped > this is weird')
account_to_account_holder["accountHolder_id"] = account_to_account_holder["accountHolder_id"].astype(int)

# temporary mapping for the Acquiring Account
mapping_tmp = account_to_account_holder.copy()
mapping_tmp = mapping_tmp.rename(columns={"id": "acquiringAccount_id",
                                          "accountHolder_id": "acquiringAccountHolder_id"})

#print('merging acquiring account')
aggregated_transactions = aggregated_transactions.merge(mapping_tmp, how="left", on="acquiringAccount_id", indicator=True)
stat = aggregated_transactions.groupby('_merge')['acquiringAccount_id'].count()
#print(stat)
aggregated_transactions = aggregated_transactions.drop(columns=['_merge'])

# temporary mapping for the Transferring Account
mapping_tmp = account_to_account_holder.copy()
mapping_tmp = mapping_tmp.rename(columns={"id": "transferringAccount_id",
                                          "accountHolder_id": "transferringAccountHolder_id"})

#print('merging acquiring account')
aggregated_transactions = aggregated_transactions.merge(mapping_tmp, how="left", on="transferringAccount_id", indicator=True)
stat = aggregated_transactions.groupby('_merge')['acquiringAccount_id'].count()
#print(stat)
aggregated_transactions = aggregated_transactions.drop(columns=['_merge'])

# drop all transactions internal to an Account Holder
internal_transactions_sel = aggregated_transactions.acquiringAccountHolder_id == aggregated_transactions.transferringAccountHolder_id
print('total number of found transactions', len(aggregated_transactions))
# aggregated_transactions = aggregated_transactions[~internal_transactions_sel]
print('  > of which non-internal', len(aggregated_transactions))

# ---------------------------------------------------------------------------------------------------
# Get ETS prices
# ---------------------------------------------------------------------------------------------------

prices = pd.read_csv(paths.path_data / "ets.csv")
prices["date"] = pd.to_datetime(prices["date"])
prices = prices[["date", "ets_price"]]

fig, ax = plt.subplots(figsize=(10,7))
ax.plot(prices.set_index("date"))
ax.grid()
ax.set_ylabel("price [EUR/t]")
ax.set_title("price of EU ETS greenhouse emission allowances")
plt.tight_layout()
plt.savefig('../plots/ets_prices.png')
plt.close()

# ---------------------------------------------------------------------------------------------------
# Price the transactions
# ---------------------------------------------------------------------------------------------------

aggregated_transactions = aggregated_transactions.merge(prices, how='left', on="date", indicator=True)
stat = aggregated_transactions.groupby('_merge')['acquiringAccount_id'].count()
#print(stat)
aggregated_transactions = aggregated_transactions.drop(columns=['_merge'])

eu_allocation_account_id = session.query(Account).filter(Account.name == "EU EU ALLOCATION ACCOUNT").first().id
eu_deletion_account_id = session.query(Account).filter(Account.name == "EU EU Allowance deletion").first().id

free_allocation = aggregated_transactions["transferringAccount_id"] == eu_allocation_account_id
deletion = aggregated_transactions["acquiringAccount_id"] == eu_deletion_account_id
aggregated_transactions.loc[free_allocation, "ets_price"] = 0.0
aggregated_transactions.loc[deletion, "ets_price"] = 0.0

aggregated_transactions["value"] = aggregated_transactions["ets_price"] * aggregated_transactions["amount_directed"]


# ---------------------------------------------------------------------------------------------------
# Compute and plot cumulative volumes and costs
# ---------------------------------------------------------------------------------------------------

# cumulative_value = aggregated_transactions[["date", "value"]]
# cumulative_value = cumulative_value.groupby("date").sum()
# cumulative_value = cumulative_value.cumsum()

cumulative_amount = aggregated_transactions[["date", "amount_directed"]]
cumulative_amount = cumulative_amount.groupby("date").sum()
cumulative_amount = cumulative_amount.cumsum()

print(cumulative_amount.head())

cumulative_amount_zoom = cumulative_amount[(cumulative_amount.index > dt.datetime(2015, 5, 1)) &
                                           (cumulative_amount.index < dt.datetime(2015, 7, 1))]

fig, ax = plt.subplots(figsize=(10, 7))
ax.plot(cumulative_amount / 1e6, ds="steps-post")
ax.grid()
ax.set_ylabel("cum. amount of allowances [Mio. t CO2e]")
ax.set_title(account_holder.name)
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig('../plots/cum_vol.png')
plt.close()

fig, ax = plt.subplots(figsize=(10, 7))
ax.plot(cumulative_amount_zoom / 1e6, ds="steps-post")
ax.grid()
ax.set_ylabel("cum. amount of allowances [Mio. t CO2e]")
ax.set_title(account_holder.name)
fig.autofmt_xdate()
plt.tight_layout()
plt.savefig('../plots/cum_vol_zoom.png')
plt.close()

