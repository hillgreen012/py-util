#!/usr/bin/env python

NDEBUG = False

def Print(str):
    if NDEBUG:
        return
    print '\033[31m',
    print str,
    print '\033[0m'
