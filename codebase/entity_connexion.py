
import numpy as np
import pandas as pd
import warnings
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from eutl_orm import DataAccessLayer
from eutl_orm import Account, AccountHolder
from attic.connection_settings import connectionSettings
from attic.constants import EntityType
from attic import paths

dal = DataAccessLayer(**connectionSettings)
session = dal.session


class EntityConnexion:
    def __init__(self, entity_type, entity_id, period=(None, None)):
        print(entity_type)
        if entity_type not in [EntityType.Account, EntityType.AccountHolder, EntityType.Company]:
            raise ValueError('entity_type needs to be one of Account, Company or AccountHolder')
        else:
            self.entity_type = entity_type
        self.entity_id = entity_id
        self.period = period
        self.accounts = None
        self.transactions = None
        self.entity_name = None

        self.get_accounts()
        self.get_transactions()
        self.plot_arrows()

    def get_accounts(self):
        # look up list of accounts and return it
        if self.entity_type == 'Account':
            self.accounts = session.query(Account).filter(Account.id == self.entity_id).all()
            self.entity_name = self.accounts[0].name
        elif self.entity_type == 'AccountHolder':
            self.accounts = session.query(Account).filter(Account.accountHolder_id == self.entity_id).all()
            self.entity_name = session.query(AccountHolder).filter(AccountHolder.id == self.entity_id).first().name
        elif self.entity_type == 'Company':
            self.accounts = session.query(Account).filter(Account.companyRegistrationNumber == self.entity_id).all()
            self.entity_name = ''

        print(f'You are investigating {self.entity_type} number {self.entity_id} '
              f'(name: {self.entity_name}) - {len(self.accounts)} related accounts')

    def get_transactions(self):
        transaction_tables = []
        for account in self.accounts:
            transaction_tables.append(account.get_transactions())

        if len(transaction_tables) == 1 and transaction_tables[0] is None:
            return
        transactions = pd.concat(transaction_tables)
        transactions['datetime'] = pd.to_datetime(transactions.index)

        column_list = ['datetime', 'amount',
                       'transferringAccount_id', 'transferringAccountName', 'transferringAccountType',
                       'acquiringAccount_id', 'acquiringAccountName', 'acquiringAccountType',
                       'transactionTypeMain', 'transactionTypeSupplementary', 'unitType']
        for c in column_list:
            if c not in transactions.columns:
                warnings.warn('Columns not available. Investigate!')
                return

        transactions = transactions[column_list].copy()

        if self.entity_type == 'Account':
            for v in ['transferring', 'acquiring']:
                transactions.rename(columns={f'{v}Account_id': f'{v}Entity_id',
                                             f'{v}AccountName': f'{v}Entity_name',
                                             f'{v}AccountType': f'{v}Entity_type'}, inplace=True)
        elif self.entity_type == 'AccountHolder':
            # todo: merge with account get accountholder_id > merger with accountHolder (transferring and acquiring) > keep accountholder name
            #  then rename columns, drop transactions from accountholder_id to itself
            pass
        elif self.entity_type == 'Company':
            # todo: merge with account get company_id > merger with accountHolder (transferring and acquiring)
            #  then rename columns
            pass

        # todo: add an option to remove "admin" transactions

        self.transactions = transactions

        print('  > Found {} transactions\n'.format(len(self.transactions)))
        # todo: add the period condition here

    def plot_arrows(self):

        if self.transactions is None:
            return

        df = self.transactions
        this_node = self.entity_id

        transaction_graph = nx.from_pandas_edgelist(df, source='transferringEntity_id', target='acquiringEntity_id',
                                    edge_attr='amount', create_using=nx.DiGraph())
        if len(transaction_graph) > 40:  # if too many nodes, no point in plotting
            warnings.warn('Too many nodes, not producing graph')
            warnings.warn('Too many nodes, not producing graph')
            return

        # determine receiver/sender/trader status
        attrs = {}
        trans_entities = set(df['transferringEntity_id'])
        acqui_entities = set(df['acquiringEntity_id'])

        # colors
        color_legend = {'this': 'green', 'trader': 'violet', 'sender': 'blue', 'receiver': 'red'}
        color_handles = []
        for c in color_legend:
            color_handles.append(mpatches.Patch(color=color_legend[c], label=c))

        # loop over nodes
        for node in transaction_graph:
            attrs[node] = {}
            # todo: change color based on account type not on trader_type
            if (node in trans_entities) and (node in acqui_entities):
                attrs[node]['trader_type'] = 'trader'
                attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
                attrs[node]['type'] = df[df['transferringEntity_id'] == node].iloc[0]['transferringEntity_type']
                attrs[node]['name'] = df[df['transferringEntity_id'] == node].iloc[0]['transferringEntity_name']
                attrs[node]['id'] = df[df['transferringEntity_id'] == node].iloc[0]['transferringEntity_id']
            elif node in trans_entities:
                attrs[node]['trader_type'] = 'sender'
                attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
                attrs[node]['type'] = df[df['transferringEntity_id'] == node].iloc[0]['transferringEntity_type']
                attrs[node]['name'] = df[df['transferringEntity_id'] == node].iloc[0]['transferringEntity_name']
                attrs[node]['id'] = df[df['transferringEntity_id'] == node].iloc[0]['transferringEntity_id']
            elif node in acqui_entities:
                attrs[node]['trader_type'] = 'receiver'
                attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
                attrs[node]['type'] = df[df['acquiringEntity_id'] == node].iloc[0]['acquiringEntity_type']
                attrs[node]['name'] = df[df['acquiringEntity_id'] == node].iloc[0]['acquiringEntity_name']
                attrs[node]['id'] = df[df['acquiringEntity_id'] == node].iloc[0]['acquiringEntity_id']
            if node == this_node:  # just changing the trader type and color (name and type should have been set before)
                attrs[node]['trader_type'] = 'this'
                attrs[node]['color'] = color_legend[attrs[node]['trader_type']]

        nx.set_node_attributes(transaction_graph, attrs)

        # defining width of arrows
        width_thinnest_edge = 0.05
        width_thickest_edge = 3
        max_width = max([transaction_graph[u][v]['amount'] for u, v in transaction_graph.edges])
        width = [width_thickest_edge * transaction_graph[u][v]['amount'] / max_width + width_thinnest_edge for u, v in transaction_graph.edges()]

        # get list of nodes and reorder based on trader type
        list_of_nodes = [x for _, x in sorted(zip([attrs[n]['trader_type'] for n in transaction_graph], transaction_graph))]
        list_of_nodes.remove(this_node)

        # define circular position
        pos = nx.circular_layout(list_of_nodes, scale=2)
        pos[this_node] = np.array([0, 0])

        # define label positions (slightly below node)
        pos_attrs = {}
        for node, coords in pos.items():
            pos_attrs[node] = (coords[0], coords[1] - .25)

        # plot the whole thing
        fig, ax = plt.subplots(figsize=(10, 7))
        nx.draw(transaction_graph, pos=pos,
                connectionstyle='arc3,rad=0.1', node_color=[attrs[x]['color'] for x in attrs],
                with_labels=False, width=width)
        nx.draw_networkx_labels(transaction_graph, pos_attrs, labels={n: f"{attrs[n]['name']} \n({attrs[n]['id']})" for n in attrs})
        ax.legend(handles=color_handles)
        if self.entity_type == 'Company':
            plt.title(f'ETS trading connections for {self.entity_type}: {self.entity_id}')
        else:
            plt.title(f'ETS trading connections for {self.entity_type}: {self.entity_name}')
        plt.tight_layout()
        print(paths.path_plots / f'arrows')
        plt.savefig(f'../plots/arrows_{self.entity_type}_{self.entity_id}.png', dpi=500)
        # plt.savefig(paths.path_plots / f'/arrows_{self.entity_type}_{self.entity_id}.png', dpi=500)
        plt.close()

    def plot_cumul(self):
        # todo: add the plot of cumulative holdings
        return

    def plot_compliance(self):
        # todo: add the plot of emissions, free allocations and surrendered certificates
        return
