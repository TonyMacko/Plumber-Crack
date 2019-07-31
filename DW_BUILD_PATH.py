# -*- coding: utf-8 -*-
"""
Created on Mon Feb 25 15:57:18 2019

@author: anthony.macko
"""

import socket
HOSTNAME = socket.gethostname()
if HOSTNAME == "CTXAMACKO":
    BASEPATH = "S:"
    BASEPATHEXT = "X:"
else:
    BASEPATH = "/shares/live/sisense"
    BASEPATHEXT = "/shares/live/sisense/extended"
