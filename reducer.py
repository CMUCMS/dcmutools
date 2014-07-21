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

from globals import *
from dscp import dscp

class Reducer(object):
    """
    Base class for reducer servers.
    Protocol:
      SRV: send target directory
      CLT: scp to target directory, send file name
    """

    def __init__(self, targetFileName_, targetDirName_, maxSize = 0, workdir = '', log = print):
        self._logFunc = log
        
        self._targetFileBase = targetFileName_[0:targetFileName_.rfind('.')]
        self._targetFileSuffix = targetFileName_[targetFileName_.rfind('.'):]
        self._targetDirName = targetDirName_
        self._maxSize = maxSize
        
        self._outputNumber = 0

        self.inputQueue = Queue.Queue()

        self.workdir = workdir

        if not self.workdir:
            while True:
                self.workdir = TMPDIR + '/' + string.join(random.sample(string.ascii_lowercase, 6), '')
                try:
                    os.mkdir(self.workdir)
                    break
                except OSError:
                    pass

        try:
            os.mkdir(self.workdir + '/input')
        except OSError:
            if not os.path.isdir(self.workdir + '/input'):
                raise RuntimeError("Reducer input directory")
        
        if DEBUG: self._logFunc('Working directory', self.workdir)
        
        self.result = {'failed': []}

    def setLog(self, logFunc_):
        self._logFunc = logFunc_

    def nProcessed(self):
        return reduce(lambda n, l: n + len(l), self.result.values(), 0)

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

            total = self.nProcessed() + len(remaining)
            while self.nProcessed() != total:
                self.reduce()

        suffix = '_' + str(self._outputNumber) + self._targetFileSuffix
        
        if self._outputNumber == 0:
            try:
                oldName = self.workdir + '/' + self._targetFileBase + '_0' + self._targetFileSuffix
                newName = self.workdir + '/' + self._targetFileBase + self._targetFileSuffix
                self._logFunc('mv', oldName, newName)
                os.rename(oldName, newName)

                self.result[newName] = self.result[oldName]
                self.result[oldName] = []

                suffix = self._targetFileSuffix

            except:
                self._logFunc('Exception in renaming ouput:', sys.exc_info()[0:2])
                pass

        self.export(suffix)

        self._logFunc('Result:', str(self.result))

    def export(self, suffix, cleanup = True):
        outputFile = self.workdir + '/' + self._targetFileBase + suffix

        self._logFunc('Copying ' + outputFile + ' to', self._targetDirName)

        if ':' in self._targetDirName:
            proc = subprocess.Popen(['scp', '-oStrictHostKeyChecking=no', '-oLogLevel=quiet', outputFile, self._targetDirName],
                stdin = subprocess.PIPE,
                stdout = subprocess.PIPE,
                stderr = subprocess.STDOUT)

            while proc.poll() is None: time.sleep(10)

            if proc.returncode != 0:
                while True:
                    line = proc.stdout.readline().strip()
                    if not line: break
                    self._logFunc(line)

                self.result['failed'] += self.result[outputFile]
                self.result[outputFile] = []

                return False
            
        else:
            try:
                if self._targetDirName[0:7] == '/store/':
                    copyFunc = dscp
                else:
                    copyFunc = shutil.copyfile
                
                if copyFunc(outputFile, self._targetDirName + '/' + os.path.basename(outputFile)) is False: # shutil.copyfile returns None
                    raise RuntimeError('copyFunc failed')

            except:
                self._logFunc('Exception in copying output:', sys.exc_info()[0:2])

                self.result['failed'] += self.result[outputFile]
                self.result[outputFile] = []

                return False

        if cleanup:
            os.remove(outputFile)

    def cleanup(self):
        if len(self.result['failed']) == 0:
            shutil.rmtree(self.workdir, ignore_errors = True)


class Hadder(Reducer):
    """
    Reducer using TFileMerger.
    """
    # TODO make this multi-thread

    ROOT = None

    def __init__(self, targetFileName_, targetDirName_, maxSize = 0, workdir = '', log = print):
        if Hadder.ROOT is None:
            argv = sys.argv
            sys.argv = ['', '-b']
            import ROOT
            Hadder.ROOT = ROOT
            Hadder.ROOT.gSystem.Load("libTreePlayer.so")
            sys.argv = argv # sys.argv is read when a first call to ROOT object is made
        
        Reducer.__init__(self, targetFileName_, targetDirName_, maxSize, workdir)

        self._lock = threading.Lock()

    def reduce(self):
        with self._lock:
            tmpOutputPath = self.workdir + '/tmp' + self._targetFileSuffix
            merger = Hadder.ROOT.TFileMerger(False, False)
        
            if not merger.OutputFile(tmpOutputPath):
                self._logFunc('Cannot open temporary output', tmpOutputPath)
                return
   
            outputPath = self.workdir + '/' + self._targetFileBase + '_' + str(self._outputNumber) + self._targetFileSuffix

            if outputPath not in self.result:
                self.result[outputPath] = []

            totalSize = 0
        
            if os.path.exists(outputPath):
                if not merger.AddFile(outputPath):
                    self._logFunc('Cannot append to', outputPath)
                    return

                self._logFunc('Appending to', outputPath)

                totalSize += os.stat(outputPath).st_size

            outputFull = True

            toAdd = []
            while True:
                try:
                    inputName = self.inputQueue.get(block = False) # will raise Queue.Empty exception when empty
                    inputPath = self.workdir + '/input/' + inputName

                    totalSize += os.stat(inputPath).st_size
                    if self._maxSize != 0 and totalSize / 1048576 >= self._maxSize:
                        self.inputQueue.put(inputName)
                        break

                    if not merger.AddFile(inputPath):
                        self._logFunc('Cannot add', inputPath, 'to list')
                        self.result['failed'].append(inputPath)
                        continue
                    
                    toAdd.append(inputPath)
                    
                except Queue.Empty:
                    # loop over inputPaths reached the end -> no need to increment outputNumber
                    outputFull = False
                    break
                except:
                    self.result['failed'].append(inputPath)
                    raise

            if not os.path.exists(outputPath) and len(toAdd) == 1:
                self._logFunc('mv', toAdd[0], tmpOutputPath)
                os.rename(toAdd[0], tmpOutputPath)
                success = True
            else:
                self._logFunc('hadd', tmpOutputPath, string.join(toAdd))
                success = merger.Merge()

            if success:
                self._logFunc('mv', tmpOutputPath, outputPath)
                os.rename(tmpOutputPath, outputPath)
                self.result[outputPath] += toAdd
            else:
                self._logFunc('Merge failed')
                self.result['failed'] += toAdd

            if outputFull:
                self.export('_' + str(self._outputNumber) + self._targetFileSuffix)
                self._outputNumber += 1
    

class HEfficiencyAdder(Hadder):
    """
    Hadder with efficiency calculation.
    """

    def __init__(self, targetFileName_, targetDirName_, maxSize = 0, workdir = '', log = print):
        Hadder.__init__(self, targetFileName_, targetDirName_, maxSize, workdir)

    def finalize(self):
        Reducer.finalize(self)

        if self._outputNumber != 0:
            self._logFunc('Output seems to be a tree file. Will not calculate efficiency.')
            return

        file = Hadder.ROOT.TFile(self.workdir + '/' + self._targetFileBase + self._targetFileSuffix, "update")
        for key in file.GetListOfKeys():
            if '_numer' not in key.GetName(): continue
            numer = key.ReadObj()
            if not numer.InheritsFrom(Hadder.ROOT.TH1.Class()): continue
            denom = file.Get(numer.GetName().replace('_numer', '_denom'))
            if not denom: continue

            if numer.GetDimension() == 1 and denom.GetDimension() == 1:
                eff = Hadder.ROOT.TGraphAsymmErrors(numer, denom)
                eff.SetMarkerStyle(8)
                eff.SetLineColor(Hadder.ROOT.kBlack)
                eff.SetTitle(numer.GetTitle())
                eff.GetXaxis().SetTitle(numer.GetXaxis().GetTitle())
                eff.GetYaxis().SetRangeUser(0., 1.1)
                eff.Write(numer.GetName().replace('_numer', '_eff'))
            elif numer.GetDimension() == 2 and denom.GetDimension() == 2:
                eff = numer.Clone(numer.GetName().replace('_numer', '_eff'))
                eff.Sumw2()
                eff.Divide(denom)
                eff.Write()
                
        self.export(self._targetFileSuffix)


if __name__ == '__main__':

    from optparse import OptionParser

    parser = OptionParser(usage = 'Usage: python reducer.py [OPTIONS] REDUCER WORKDIR')

    parser.add_option('-o', '--output', dest = 'outputFileName', help = 'Output path', default = 'reduced.root')
    parser.add_option('-m', '--max-size', dest = 'maxFileSize', help = 'Maximum size of each output ROOT file in MB', default = 0)

    options, args = parser.parse_args()

    if len(args) != 2:
        parser.print_usage()
        sys.exit(1)

    workdir = args[1]

    outputDir = os.path.dirname(options.outputFileName)
    if not outputDir:
        outputDir = workdir

    reducer = eval(args[0])(os.path.basename(options.outputFileName), outputDir, int(options.maxFileSize), workdir = workdir)

    for fileName in os.listdir(workdir + '/input'):
        reducer.inputQueue.put(fileName)

    reducer.reduce()
    reducer.finalize()
