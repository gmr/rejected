#!/usr/bin/env python2.6
"""
Send an opslog notification for the current user and server that queue consumers were restarted.

__author__ = "Gavin M. Roy"
__email__ = "gmr@myyearbook.com"
__since__ = "2011-04-04"
"""
from myyearbook import pgsql
from os import getlogin
from socket import gethostname
from sys import argv

if len(argv) > 1:
    notes = argv[1]
else:
    notes = 'Restarted rejected consumers'

connection = pgsql.connect("ops02", 5432, "production", "www")
cursor = pgsql.get_cursor(connection)
sql = "INSERT INTO operations.log_entries (entry_type, entry_time, entry_location, author, server, notes) VALUES ('Queue Consumer', now(), 'Production', '%s', '%s', '%s')" % (getlogin(), gethostname().split('.')[0], notes)
cursor.execute(sql)
