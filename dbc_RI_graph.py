#!/usr/bin/env python
#Developed by Dan Wagar: daniel.wagar@teradata.com, dandwagar@gmail.com

import getpass
import os
import shlex
import sys
import re
import time
from subprocess import Popen, PIPE
from collections import defaultdict

def getLogons():
    """Get logon information from user"""
    print("Enter the username you will be accessing the database with")
    user = raw_input()
    print("Enter the users password")
    password = getpass.getpass()
    return user, password


def verifyCredentials(credentials):
    """Verify successful logon with credentials provided from getLogons"""
    user, password = credentials

    f = open('chkLogon', 'w')
    f.write('.logon ' + user + ',' + password)
    f.close()

    #command1="bteq < chkLogon > logonTest.out 2>&1"
    #args1=shlex.split(command1)
    #Popen(args1).wait()
    os.system("bteq < chkLogon > logonTest.out 2>&1")
    command = "grep -o 'Logon successfully completed' logonTest.out"
    args = shlex.split(command)
    #print(args)
    p = Popen(args, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    os.remove('chkLogon')
    #os.remove('logonTest.out')
    return stdout


def retry_logon():
    print("Logon failed. Hit enter to try again or 'q' to quit")
    choice = raw_input()
    if choice == 'q' or choice == 'Q':
        sys.exit()
    else:
        #del credentials[:]
        credentials = getLogons()
        user, password = credentials
        result = verifyCredentials(user, password)

    return result;


def get_dbc_RI(credentials):
    """Submits a query to get name of every table in database where RI is to be checked"""

    user, password = credentials

    f = open('getNames', 'w')
    f.write('.logon ' + user + ',' + password + '\n')
    f.write(".export file = names_temp.out\n")
    f.write("SELECT ReferencedTblID, ReferencingTblID, ForeignKeyFID\n")
    f.write("FROM DBC.ReferencedTbls\n")
    f.write("order by 1, 2, 3;\n")
    f.write(".export reset\n")
    f.write(".quit\n")
    f.close()

    #command="bteq <getNames> ./RI_check/tableNames.sql 2>&1"
    #args=shlex.split(command)
    #Popen(args).wait()
    os.system("bteq <getNames> ./RI_check/tableNames.sql 2>&1")

    f = open( 'RI_check/tableNames.out', 'w' )
    p = Popen(['tail', '-n+3', 'names_temp.out'], stdout=f, stderr=PIPE).wait()
    f.close()
    rows = []
    with open('RI_check/tableNames.out') as f:
        rows = f.readlines()
        rows = [x.strip() for x in rows]

    os.remove('names_temp.out')
    os.remove('getNames')

    return rows

def extract_subgraphs(edge_list):
    '''Return a list of edge_lists for each unique subgraph in an edge_list.'''
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

def format(rows):

    row_cols = []
    for row in rows:
        row_cols.append(row.split())

    for i, row in enumerate(row_cols):
        for j, col in enumerate(row):
            no_change = (len(col) - 1)
            if j is not no_change:
                row_cols[i][j] = col[:-4]

    return row_cols


def tvmid_to_tblid(tvmid_edges):
    tblid_edges = []
    for edge in tvmid_edges:
        tmp = []
        for tvmid in edge:
            tmp.append('{0}{1} {2}{3}'.format(tvmid[2:4], tvmid[0:2],
                                              tvmid[6:], tvmid[4:6]))
        tblid_edges.append(tuple(tmp))

    return tblid_edges



#MAIN
credentials = getLogons()

#print(user)
#print(password)

result = verifyCredentials(credentials)
print(result)

#Loop until user submits valid credentials or chooses to quit
while result != 'Logon successfully completed\n':
    result = retry_logon()

print("Credentials Verified")
os.remove('logonTest.out')

dbc_RI = get_dbc_RI(credentials)

formatted_RI = format(dbc_RI)

RI_tvmid_edges = list(set((col[0], col[1]) for col in formatted_RI))

RI_tblid_edges = tvmid_to_tblid(RI_tvmid_edges)

for tup in RI_tblid_edges:
    print(tup)

subgraphs = extract_subgraphs(RI_tblid_edges)
for i, subgraph in enumerate(subgraphs):
    print('Subgraph {0}'.format(i))
    for edge in subgraph:
        print(edge)


sys.exit()