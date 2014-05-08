#!/usr/bin/env python

# chec_dataset.py
# usage: python check_dataset.py DATASETPATH

import re
import os
import sys

srcdir = os.path.dirname(os.path.realpath(__file__))
if srcdir not in sys.path:
    sys.path.append(srcdir)
from dsrm import rmlink

def check_dataset(path, names, exclude = [], removeEmpty = False):

    patterns = []
    for name in names:
        patterns.append(re.compile(name[0:name.rfind('.')] + '_(([0-9]+)_[0-9]+_[a-zA-Z0-9]{3})[.]' + name[name.rfind('.') + 1:]))
    
    lists = {} # {jobNumber: {suffix-1: [pat1exists, pat2exists, ..], suffix-2: [pat1exists, pat2exists, ..]}}
        
    for file in os.listdir(path):
        for iP in range(len(patterns)):
            pattern = patterns[iP]
            matches = pattern.match(file)
            if not matches: continue

            jobNumber = int(matches.group(2))

            if jobNumber in exclude:
                break

            if removeEmpty:
                pfn = os.readlink(path + '/' + file)
                try:
                    size = os.stat(pfn).st_size
                except OSError:
                    size = 0

                if size == 0:
                    print 'dsrm', path + '/' + file
                    rmlink(path + '/' + file)
                    break
            
            if jobNumber not in lists:
                lists[jobNumber] = {}

            suffix = matches.group(1)
            if suffix not in lists[jobNumber]:
                lists[jobNumber][suffix] = [False] * len(patterns)

            lists[jobNumber][suffix][iP] = True

            break
        
        else:
            print 'File', file, 'does not match any given patterns'

    incompletes = [] # list of suffices
    duplicates = [] # list of list of suffices
    
    for jobNumber, combinations in lists.items():
        for suffix, existenceList in combinations.items():
            if not reduce(lambda x, y: x and y, existenceList):
                incompletes.append(suffix)

        if len(combinations) > 1:
            duplicates.append(combinations.keys())

    absents = sorted(set(range(1, max(lists.keys()) + 1)) - set(lists.keys()))

    return incompletes, duplicates, absents

if __name__ == '__main__':

    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option('-x', '--exclude', dest = 'exclude', help = 'Jobs to exclude (comma separated, no spaces)', default = '')
    parser.add_option('-e', '--remove-empty', action = 'store_true', dest = 'removeEmpty', help = 'Remove empty files')
    parser.add_option('-p', '--patterns', dest = 'patterns', help = 'File names to match (comma separated, no spaces)', default = 'susyEvents.root')
    
    options, args = parser.parse_args()

    path = args[0]

    patterns = options.patterns.split(',')

    print 'Checking', path, 'for patterns', options.patterns.split(',')

    if options.exclude.strip():
        exclude = map(int, options.exclude.split(','))
    else:
        exclude = []

    incompletes, duplicates, absents = check_dataset(path, patterns, exclude, options.removeEmpty)

    allFiles = os.listdir(path)

    if len(incompletes):
        while True:
            print '(R)emove incomplete file sets / (s)kip?'
            response = sys.stdin.readline().strip()
            if response == 'R' or response == 's': break

        if response == 'R':
            while True:
                try:
                    suffix = incompletes.pop()
                except IndexError:
                    break

                for file in filter(lambda s: suffix in s, allFiles):
                    print 'dsrm', path + '/' + file
                    rmlink(path + '/' + file)

                resolved = []
                for iD in range(len(duplicates)):
                    if suffix in duplicates[iD]:
                        duplicates[iD].remove(suffix)
                        if len(duplicates[iD]) < 2:
                            resolved.append(duplicates[iD])

                for suffices in resolved:
                    duplicates.remove(suffices)


    if len(duplicates):
        while True:
            print '(R)emove duplicates individually / remove (All) duplicates / (s)kip / (l)ist?'
            response = sys.stdin.readline().strip()
            if response == 'R' or response == 'All' or response == 's' or response == 'l':
                break

        if response == 'R' or response == 'l':
            resolved = []
            for iD in range(len(duplicates)):
                suffices = duplicates[iD]
                print '============================='
                files = {}
                for iS in range(len(suffices)):
                    files[iS] = []
                    print '<' + str(iS) + '>'
                    for file in sorted(filter(lambda s: suffices[iS] in s, allFiles)):
                        lfn = path + '/' + file
                        pfn = os.readlink(lfn)
                        print (' %10d' % os.stat(pfn).st_size), file
                        files[iS].append(lfn)

                if response == 'R':
                    print 'Remove (space separated):'
                    indices = sys.stdin.readline().strip().split()
    
                    for index in indices:
                        try:
                            lfns = files[index]
                        except KeyError:
                            continue
    
                        for lfn in lfns:
                            print 'dsrm', lfn
                            rmlink(lfn)

                        suffices.pop(index)

                    if len(suffices) < 2:
                        resolved.append(suffices)

                for suffices in resolved:
                    duplicates.remove(suffices)

        elif response == 'All':
            for suffices in duplicates:
                suffices.pop(0)
                for suffix in suffices:
                    for file in filter(lambda s: suffix in s, allFiles):
                        print 'dsrm', path + '/' + file
                        rmlink(path + '/' + file)

            duplicates = []

    if len(absents):
        print 'Missing jobs'
        print ','.join(map(str, absents))

    if len(incompletes):
        print 'Incomplete file sets'
        for suffix in incompletes:
            print ' ', suffix

    if len(duplicates):
        print 'Duplicates'
        for suffices in duplicates:
            print ' ', suffices

