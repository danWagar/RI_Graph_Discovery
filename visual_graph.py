
#Developed by Dan Wagar: daniel.wagar@teradata.com, dandwagar@gmail.com

import teradata
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict

udaExec = teradata.UdaExec(appName="test", version="1.0",
                           logConsole=False)

session = udaExec.connect(method="odbc", system="lnx2092",
                          username="dbc", password="dbc")


def ref_ids(query):
    """ID columns returned as bytearray, so format and return as list
    of strings."""
    def bytearray_to_string(barray):
        out_string = ''.join('{:02x}'.format(x) for x in barray)
        return out_string

    formatted_rows = []
    for row in session.execute(query):
        formatted_rows.append(tuple(map(bytearray_to_string, row)))

    return formatted_rows

def id_to_name(id):
    """Translate table id references to dbname.tblname references."""

    query = """SELECT trim(d.databasename) || '.' || trim(t.tvmname)
               FROM dbc.tvm as t, dbc.dbase as d
               where t.databaseid = d.databaseid
               and t.tvmid = '{}'xb""".format(id)
    result = ''
    for row in session.execute(query):
        result = row[0]

    return result

def table_names(table_ids):
    """ Create dict key=tableid value=dbname.tblname, return new list
        mapping tblid to dbname.tblname"""
    d = defaultdict(dict)
    table_names = []
    for row in table_ids:
        temp = []
        for tblid in row:
            if id not in d:
                d[tblid] = id_to_name(tblid)
            temp.append(d[tblid])
        tup = tuple(temp)
        table_names.append(tup)

    return table_names

def ref_dict(table_names):
    '''pass'''
    d = defaultdict(dict)

    d['to'] = []
    d['from'] = []
    for ref in table_names:
        d['to'].append(ref[1])
        d['from'].append(ref[0])

    return d

def extract_subgraphs(edge_list):
    '''Return a list of edge_lists for each unique connected subgraph in an edge_list.'''
    e = edge_list[:]
    subgraphs = []
    while len(e) is not 0:
        temp_edges = [e.pop(0)]
        sub_edge_list = []
        nodes_checked = []
        while len(temp_edges) is not 0:
            x = temp_edges.pop(0)
            sub_edge_list.append(x)
            for node in x:
                if node in nodes_checked:
                    continue
                else:
                    nodes_checked.append(node)
                temp = e[:]
                count = 0
                for i, edge in enumerate(e):
                    if node in edge:
                        temp_edges.append(edge)
                        del temp[i - count]
                        count += 1
                e = temp

        subgraphs.append(sub_edge_list)

    return subgraphs


#MAIN
'''The logic to get relations by table id and translate to relations by
table name could be done in a single query.  The downside is that the
query can be complex and potentially have large in-lists.  Complex queries
with large in-lists are known cause EVL stack overflow and can be expensive
to run, so lets do it programatically with a series of simple queries for the
id to name translation'''
query = """SELECT DISTINCT referencedTblId, referencingTblId
           FROM DBC.referencedTbls"""

ref_ids = ref_ids(query)

ref_names = table_names(ref_ids)

subgraphs = extract_subgraphs(ref_names)

print(len(subgraphs))
print(subgraphs)
print(len(ref_names))
#G.add_edges_from(subgraphs[0])

for graph in subgraphs:

    G = nx.DiGraph()
    G.add_edges_from(graph)

    nx.draw(G,
            pos=nx.shell_layout(G),
            with_labels=True,
            node_size=1000,
            alpha=0.3,
            arrows=True,
            arrowstyle='->',
            arrowsize=20)

    plt.show()
    #plt.gcf().clear()

exit()