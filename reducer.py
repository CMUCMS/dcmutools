#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import random
import time
import string
import glob
import shutil
import Queue
import threading
import subprocess

srcdir = os.path.dirname(os.path.realpath(__file__))
if srcdir not in sys.path:
    sys.path.append(srcdir)
from globals import *
from dscp import dscp

class Reducer(object):
    """
    Base class for reducer servers.
    Protocol:
      SRV: send target directory
      CLT: scp to target directory, send file name
    """

    def __init__(self, targetFileName_, maxSize = 0, workdir = '', log = print):
        self._logFunc = log
        
        self._targetFileBase = targetFileName_[0:targetFileName_.rfind('.')]
        self._targetFileSuffix = targetFileName_[targetFileName_.rfind('.'):]
        self._maxSize = maxSize
        
        self._outputNumber = 0

        self.inputQueue = Queue.Queue()

        self.workdir = workdir

        if not self.workdir:
            while True:
                self.workdir = TMPDIR + '/' + string.join(random.sample(string.ascii_lowercase, 6), '')
                try:
                    os.mkdir(self.workdir)
                except OSError:
                    pass

        try:
            os.mkdir(self.workdir + '/input')
        except OSError:
            if not os.path.isdir(self.workdir + '/input'):
                raise RuntimeError("Reducer input directory")
        
        if DEBUG: self._logFunc('Working directory', self.workdir)
        
        self.succeeded = []
        self.failed = []

    def setLog(self, logFunc_):
        self._logFunc = logFunc_

    def reduce(self):
        pass

    def finalize(self):
        remaining = []
        while True:
            try:
                remaining.append(self.inputQueue.get(block = False))
            except Queue.Empty:
                break

        if len(remaining) != 0:
            for entry in remaining:
                self.inputQueue.put(entry)

            total = len(self.succeeded) + len(self.failed) + len(remaining)
            while len(self.succeeded) + len(self.failed) != total:
                self.reduce()
        
        if self._outputNumber == 0:
            try:
                self._logFunc('mv', self.workdir + '/' + self._targetFileBase + '_0' + self._targetFileSuffix, self.workdir + '/' + self._targetFileBase + self._targetFileSuffix)
                os.rename(self.workdir + '/' + self._targetFileBase + '_0' + self._targetFileSuffix,
                          self.workdir + '/' + self._targetFileBase + self._targetFileSuffix)
            except:
                self._logFunc('Exception in renaming ouput:', sys.exc_info()[0:2])
                pass

    def copyOutputTo(self, destination_):
        self._logFunc('Copying output to', destination_)
        if ':' in destination_:
            for outputFile in glob.glob(self.workdir + '/' + self._targetFileBase + '*' + self._targetFileSuffix):
                proc = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', '-oLogLevel=quiet', outputFile, destination_],
                    stdin = subprocess.PIPE,
                    stdout = subprocess.PIPE,
                    stderr = subprocess.STDOUT)
    
                while proc.poll() is None: time.sleep(10)

                if proc.returncode != 0:
                    while True:
                        line = proc.stdout.readline().strip()
                        if not line: break
                        self._logFunc(line)

                    return False
            
            return True
            
        else:
            try:
                if destination_[0:7] == '/store/':
                    copyFunc = dscp
                else:
                    os.rename
                
                for outputFile in glob.glob(self.workdir + '/' + self._targetFileBase + '*' + self._targetFileSuffix):
                    copyFunc(outputFile, destination_ + '/' + os.path.basename(outputFile))

                return True
            except:
                return False

    def cleanup(self):
        if len(self.failed) == 0:
            shutil.rmtree(self.workdir, ignore_errors = True)


class Hadder(Reducer):
    """
    Reducer using TFileMerger.
    """
    # TODO make this multi-thread

    ROOT = None

    def __init__(self, targetFileName_, maxSize = 0, workdir = '', log = print):
        if Hadder.ROOT is None:
            argv = sys.argv
            sys.argv = ['', '-b']
            import ROOT
            Hadder.ROOT = ROOT
            Hadder.ROOT.gSystem.Load("libTreePlayer.so")
            sys.argv = argv # sys.argv is read when a first call to ROOT object is made
        
        Reducer.__init__(self, targetFileName_, maxSize, workdir)

        self._lock = threading.Lock()

    def reduce(self):
        with self._lock:
            tmpOutputPath = self.workdir + '/tmp' + self._targetFileSuffix
            merger = Hadder.ROOT.TFileMerger(False, False)
        
            if not merger.OutputFile(tmpOutputPath):
                self._logFunc('Cannot open temporary output', tmpOutputPath)
                return
   
            outputPath = self.workdir + '/' + self._targetFileBase + '_' + str(self._outputNumber) + self._targetFileSuffix
        
            if os.path.exists(outputPath):
                if not merger.AddFile(outputPath):
                    self._logFunc('Cannot append to', outputPath)
                    return

                self._logFunc('Appending to', outputPath)

            totalSize = 0
            toAdd = []
            while True:
                try:
                    inputPath = self.workdir + '/input/' + self.inputQueue.get(block = False) # will raise Queue.Empty exception when empty
                    if not merger.AddFile(inputPath):
                        self._logFunc('Cannot add', inputPath, 'to list')
                        self.failed.append(inputPath)
                        continue
                    
                    toAdd.append(inputPath)
                    totalSize += os.stat(inputPath).st_size
                    if self._maxSize != 0 and totalSize / 1048576 >= self._maxSize: break
                    
                except Queue.Empty:
                    # loop over inputPaths reached the end -> no need to increment outputNumber
                    self._outputNumber -= 1
                    break
                except:
                    self.failed.append(inputPath)
                    raise

            self._logFunc('hadd', tmpOutputPath, string.join(toAdd))

            if merger.Merge():
                self._logFunc('mv', tmpOutputPath, outputPath)
                os.rename(tmpOutputPath, outputPath)
                self.succeeded += toAdd
            else:
                self._logFunc('Merge failed')
                self.failed += toAdd
    
            self._outputNumber += 1
    

class HEfficiencyAdder(Hadder):
    """
    Hadder with efficiency calculation.
    """

    def __init__(self, targetFileName_, maxSize = 0, workdir = '', log = print):
        Hadder.__init__(self, targetFileName_, maxSize, workdir)

    def finalize(self):
        Reducer.finalize(self)

        if self._outputNumber != 0:
            self._logFunc('Output seems to be a tree file. Will not calculate efficiency.')

        file = Hadder.ROOT.TFile(self.workdir + '/' + self._targetFileBase + self._targetFileSuffix, "update")
        for key in file.GetListOfKeys():
            if '_numer' not in key.GetName(): continue
            numer = key.ReadObj()
            if not numer.InheritsFrom(Hadder.ROOT.TH1.Class()): continue
            denom = file.Get(numer.GetName().replace('_numer', '_denom'))
            if not denom: continue
            eff = Hadder.ROOT.TGraphAsymmErrors(numer, denom)
            eff.SetMarkerStyle(8)
            eff.SetLineColor(Hadder.ROOT.kBlack)
            eff.SetTitle(numer.GetTitle())
            eff.GetXaxis().SetTitle(numer.GetXaxis().GetTitle())
            eff.GetYaxis().SetRangeUser(0., 1.1)
            eff.Write(numer.GetName().replace('_numer', '_eff'))


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print('Usage: python reducer.py REDUCER WORKDIR [MAXSIZE]')
        sys.exit(1)

    workdir = sys.argv[2]

    try:
        maxSize = int(sys.argv[3])
    except IndexError:
        maxSize = 0

    sys.stdout.write('Output file name?: ')
    sys.stdout.flush()
    targetFileName = sys.stdin.readline().strip()

    reducer = eval(sys.argv[1])(targetFileName, maxSize, workdir)

    for file in os.listdir(workdir + '/input'):
        reducer.inputQueue.put(file)

    reducer.reduce()
    reducer.finalize()
