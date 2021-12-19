import pandas as pd
import os
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

os.chdir('C:/Users/lmillischer/PycharmProjects/eutl_orm')

plot1 = False
plot2 = True

# read data
df = pd.read_csv('data/transactions_for_arrowgraph.csv', sep=';')

if plot1:

    edges = zip(df['acquiringAccount_id'].tolist(), df['transferringAccount_id'].tolist())

    gr = nx.DiGraph()

    weights = []
    for i in range(len(df)):
        #gr.add_edge(df.iloc[i]['acquiringAccount_id'], df.iloc[i]['transferringAccount_id'], weight=1/df.iloc[i]['amount'])
        # todo: what does the edge weight do? it seems to influence the distance between nodes
        gr.add_edge(df.iloc[i]['acquiringAccount_id'], df.iloc[i]['transferringAccount_id'])
        weights.append(df.iloc[i]['amount'])

    weights = np.array(weights)
    weights = weights / weights.max() * 4
    pos = nx.circular_layout(G, scale=2)
    nx.draw(gr, node_size=500, with_labels=True, width=weights, connectionstyle="arc3,rad=0.1")
    plt.savefig('plots/omv_arrows_1.png', dpi=500)
    # plt.show()

if plot2:

    this_node = 494


    G = nx.from_pandas_edgelist(df, source='transferringAccount_id', target='acquiringAccount_id',
                                edge_attr='amount', create_using=nx.DiGraph())

    # determine receiver/sender/trader status
    attrs = {}
    trans_accounts = set(df['transferringAccount_id'].to_list())
    acqui_accounts = set(df['acquiringAccount_id'].to_list())

    # colors
    color_legend = {'this': 'green', 'trader': 'violet', 'sender': 'blue', 'receiver': 'red'}
    color_handles = []
    for c in color_legend:
        color_handles.append(mpatches.Patch(color=color_legend[c], label=c))

    # loop over nodes
    for node in G:
        attrs[node] = {}
        if (node in trans_accounts) and (node in acqui_accounts):
            attrs[node]['trader_type'] = 'trader'
            attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
            attrs[node]['type'] = df[df['transferringAccount_id'] == node].iloc[0]['transferringAccount_type']
            attrs[node]['name'] = df[df['transferringAccount_id'] == node].iloc[0]['transferringAccount_name']
        elif node in trans_accounts:
            attrs[node]['trader_type'] = 'sender'
            attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
            attrs[node]['type'] = df[df['transferringAccount_id'] == node].iloc[0]['transferringAccount_type']
            attrs[node]['name'] = df[df['transferringAccount_id'] == node].iloc[0]['transferringAccount_name']
        elif node in acqui_accounts:
            attrs[node]['trader_type'] = 'receiver'
            attrs[node]['color'] = color_legend[attrs[node]['trader_type']]
            attrs[node]['type'] = df[df['acquiringAccount_id'] == node].iloc[0]['acquiringAccount_type']
            attrs[node]['name'] = df[df['acquiringAccount_id'] == node].iloc[0]['acquiringAccount_name']
        if node == this_node:  # just changing the trader type and color (name and type should have been set before)
            attrs[node]['trader_type'] = 'this'
            attrs[node]['color'] = color_legend[attrs[node]['trader_type']]

    nx.set_node_attributes(G, attrs)

    # defining width of arrows
    minw = 0.05
    maxw = 3
    max_width = max([G[u][v]['amount'] for u, v in G.edges])
    width = [maxw*G[u][v]['amount']/max_width + minw for u, v in G.edges()]

    # get list of nodes and reorder based on trader type
    list_of_nodes = [x for _, x in sorted(zip([attrs[n]['trader_type'] for n in G], G))]
    list_of_nodes.remove(this_node)

    # define circular position
    pos = nx.circular_layout(list_of_nodes, scale=2)
    pos[494] = np.array([0, 0])

    # define label positions
    pos_attrs = {}
    for node, coords in pos.items():
        pos_attrs[node] = (coords[0], coords[1] + .25)

    # plot the whole thing
    fig, ax = plt.subplots(figsize=(10, 7))
    nx.draw(G, pos=pos,
            connectionstyle='arc3,rad=0.1', node_color=[attrs[x]['color'] for x in attrs],
            with_labels=False, width=width)
    nx.draw_networkx_labels(G, pos_attrs, labels={n: attrs[n]['name'] for n in attrs})
    ax.legend(handles=color_handles)
    plt.title('ETS trading connections for {}'.format(this_node))
    plt.savefig('plots/omv_arrows_1.png', dpi=500)

# todo: label above node
