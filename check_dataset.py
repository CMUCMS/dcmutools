#!/usr/bin/env python

# chec_dataset.py
# usage: python check_dataset.py DATASETPATH

import re
import os
import sys

path = sys.argv[1]

files = os.listdir(path)

eventsPat = re.compile(r'susyEvents_(([0-9]+)_[0-9]+_[a-zA-Z0-9]{3})')
triggersPat = re.compile(r'susyTriggers_(([0-9]+)_[0-9]+_[a-zA-Z0-9]{3})')

events = []
triggers = []

for file in files:
    eventsMatch = eventsPat.match(file)
    if eventsMatch:
        events.append((int(eventsMatch.group(2)), eventsMatch.group(1)))
        continue

    triggersMatch = triggersPat.match(file)
    if triggersMatch:
        triggers.append((int(triggersMatch.group(2)), triggersMatch.group(1)))
        continue

matchTriggers = False
if len(triggers) != 0:
    matchTriggers = True

events.sort()
triggers.sort()

currentJobNum = 0
jobStack = []
for e in events:
    iJob = e[0]
    jobId = e[1]
    
    if iJob == currentJobNum:
        jobStack.append(jobId)
    else:
        if len(jobStack) > 1:
            message = "Duplicated event files "
            for d in jobStack:
                message += d + ' '
            
            print message

        while iJob > currentJobNum + 1:
            print "Skipped event file " + str(currentJobNum + 1)
            currentJobNum += 1

        currentJobNum += 1

        jobStack = [jobId]

    if matchTriggers and e not in triggers:
        print "Missing trigger file " + jobId

if len(jobStack) > 1:
    message = "Duplicated event files "
    for d in jobStack:
        message += d + ' '
        
    print message

print "Last event file num " + str(currentJobNum)

if matchTriggers:
    for t in triggers:
        if t not in events:
            print "Orphan trigger file " + t[1]

