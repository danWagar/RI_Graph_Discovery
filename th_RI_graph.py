#!/usr/bin/env python
#Developed by Dan Wagar: daniel.wagar@teradata.com, dandwagar@gmail.com

import os
import shlex
import sys
import re
import time
from subprocess import Popen, PIPE
from collections import defaultdict


def check_screens():
    """Verify free Screen available and that Filer is not already running"""

    err = ''
    success = False

    p = Popen(['cnscim', '-s'], stdout=PIPE)
    stdout, stderr = p.communicate()
    lines = re.findall(r'Screen', stdout)
    if len(lines) == 7:
        err = '''There are no free CNS Screens.  Free up Screens and
                 re-run the script.  Good bye'''

    filerCheck = re.findall(r'Filer', stdout, re.IGNORECASE)
    if len(filerCheck) > 0:
        err = '''WARNING: There are filer sessions in progress.\n
                 Please exit these sessions before running this script.
                 Good bye.'''

    if err is '':
        success = True

    return success, err

def get_screen():
    '''Get the cnsterm screen where our filer session is running'''

    err = ''
    screen = 0
    p = Popen(['cnscim', '-s'], stdout=PIPE)
    stdout, stderr = p.communicate()
    lines = re.findall(r'Screen\s[1-4].*Filer', stdout, re.IGNORECASE)
    if len(lines) > 1:
        err = '''There is more than one filer session open, unable to
                 determine our working screen.'''
    elif len(lines) != 0:
        cols = lines[0].split(' ')
        screen = cols[1]
    else:
        err = "No filer session found"

    return screen, err

def get_TH(table_id, screen):
    '''Returns entire contents of table header'''

    cns_cmd = '''cnsrun -screen {0} -join filer -output -nostop -command
                 "{{table/l {1} 0 *}}"'''.format(screen, table_id)
    args = shlex.split(cns_cmd)
    p = Popen(args, stdout=PIPE)
    stdout, stderr = p.communicate()

    return stdout

def discover_graph(th, processed=[], ri_dict=defaultdict(dict)):
    '''Given a table header having Reference Indexes, recursively discovers
       the set of edges for all tables in the same network/graph of the given
       table header'''

    hdr_tbl_id = re.search(r'(?:TableID:\s{3})(\w{4}\s{2}\w{4})', th).group(1)
    hdr_tbl_id = re.sub(r'\s{2}', ' ', hdr_tbl_id)
    processed.append(hdr_tbl_id)

    ri_pattern = r'(?s)Start\sof\sReference.*?\*{79}'
    regex = re.compile(ri_pattern, re.M)
    for match in regex.finditer(th):
        ri_desc = (match.group(0))

        child = re.search(r'(?:ChildEntry:)([FT])', ri_desc).group(1)

        buddy_id_dec = (re.search(r'(?:Buddy Unique Table ID:)(.{11})',
                        ri_desc).group(1)).lstrip()
        buddy_id = dec_id_hex(buddy_id_dec)

        index_group = re.search(r'(?s)(?:\s\-{6}\s{4}\n)(.*?)(\-)',
                                ri_desc, re.M).group(1)
        index_list = index_group.split('\n')
        del index_list[len(index_list) - 1]

        for index in index_list:
            field = re.search(r'(?:\s*\d*\s)(\d{4})', index).group(1)

            if child is 'T':
                if hdr_tbl_id not in ri_dict:
                    ri_dict[hdr_tbl_id] = set([(buddy_id, field)])
                else:
                    ri_dict[hdr_tbl_id].add((buddy_id, field))
            else:
                if buddy_id not in ri_dict:
                    ri_dict[buddy_id] = set([(hdr_tbl_id, field)])
                else:
                    ri_dict[buddy_id].add((hdr_tbl_id, field))

        if buddy_id not in processed:
            processed.append(buddy_id)
            buddy_th = get_TH(buddy_id, filer_screen)
            discover_graph(buddy_th, processed, ri_dict)

    return ri_dict

def dec_id_hex(id):
    '''Buddy ID's are given in decimal in table header (why??!) so we have
       to convert them to a standard hex format'''

    parts=id.split()
    hex_parts = []
    for part in parts:
        hex_parts.append(hex(int(part))[2:])

    hex_id = '{0} {1}'.format(hex_parts[0].ljust(4, '0'),
                              hex_parts[1].ljust(4, '0'))
    hex_id = hex_id.upper()

    return hex_id

def goodbye():
    close = 'cnsrun -join filer -screen 1 -command "{quit}"'
    args = shlex.split(close)
    p = Popen(args, stdout=PIPE)
    stdout, stderr = p.communicate()

    sys.exit()

#Main
filer_screen = ''
print('Setting up work space . . .')
scr_chk_rslt, scr_chk_err = check_screens()

if scr_chk_rslt is True:
    cns_open = 'cnsrun -utility filer -nostop -command "{scope 0}"'
    args = shlex.split(cns_open)
    p = Popen(args, stdout=PIPE)
    stdout, stderr = p.communicate()
    filer_screen, err = get_screen()
    count = 0
    while err is not '' and count <= 3:
        '''There can be a time delay in cnscim -s update, so lets retry a
           few times with brief wait'''
        time.sleep(1)
        filer_screen, err = get_screen()
        count += 1
    if err is not '':
        print(err)
        goodbye()
else:
    print(scr_chk_err)
    goodbye()

print("Enter table id")
in_th = raw_input()
in_parts = in_th.split()

in_th = '{0} {1}'.format(in_parts[0].ljust(4, '0'), in_parts[1].ljust(4, '0'))
in_th = in_th.upper()
tbl_hdr = get_TH(in_th, filer_screen)
ri_edge_dict = discover_graph(tbl_hdr)
ri_edge_list = []
for parent, children in ri_edge_dict.iteritems():
    for child in children:
        temp = []
        temp.append(parent)
        for val in child:
            temp.append(val)
        ri_edge_list.append(temp)

for edge in ri_edge_list:
    print(edge)

goodbye()