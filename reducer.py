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
from daserver import DAServer
from dscp import dscp

class Reducer(DAServer):
    """
    Base class for reducer servers.
    Protocol:
      SRV: send target directory
      CLT: scp to target directory, send file name
    """

    def __init__(self, name_, targetFileName_, maxSize = 0, workdir = ''):
        DAServer.__init__(self, name_)
        
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
        
        if DEBUG: self.log('Working directory', self.workdir)
        
        self.succeeded = []
        self.failed = []

    def canServe(self, jobName_):
        return 1

    def serve(self, request_, jobName_):
        try:
            request_.recv(1024)
            request_.send(self.workdir + '/input')
            response = request_.recv(1024)
            self.log('Job', jobName_, ':', response)
            if response == 'FAIL':
                raise RuntimeError('Copy failed')
        except:
            self.log('Job', jobName_, 'Reducer exception', sys.exc_info()[0:2])
            try:
                request_.send('FAILED')
            except:
                # should have a way to kill the job
                pass
            return

        request_.send('OK')
        if DEBUG: self.log('Adding', reponse, 'to reducer queue')
        self.inputQueue.put(response)

        self.reduce()

    def reduce(self):
        pass

    def finalize(self):
        if self._outputNumber == 0:
            try:
                self.log('mv', self.workdir + '/' + self._targetFileBase + '_0' + self._targetFileSuffix, self.workdir + '/' + self._targetFileBase + self._targetFileSuffix)
                os.rename(self.workdir + '/' + self._targetFileBase + '_0' + self._targetFileSuffix,
                          self.workdir + '/' + self._targetFileBase + self._targetFileSuffix)
            except:
                self.log('Exception in renaming ouput:', sys.exc_info()[0:2])
                pass

    def copyOutputTo(self, destination_):
        if DEBUG: print 'Copying output to', destination_
        self.log('Copying output to', destination_)
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
                        self.log(line)

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

    def __init__(self, name_, targetFileName_, maxSize = 0, workdir = ''):
        if Hadder.ROOT is None:
            argv = sys.argv
            sys.argv = ['', '-b']
            import ROOT
            Hadder.ROOT = ROOT
            Hadder.ROOT.gSystem.Load("libTreePlayer.so")
            sys.argv = argv # sys.argv is read when a first call to ROOT object is made
        
        Reducer.__init__(self, name_, targetFileName_, maxSize, workdir)

        self._lock = threading.Lock()

    def reduce(self):
        with self._lock:
            tmpOutputPath = self.workdir + '/tmp' + self._targetFileSuffix
            merger = Hadder.ROOT.TFileMerger(False, False)
        
            if not merger.OutputFile(tmpOutputPath):
                self.log('Cannot open temporary output', tmpOutputPath)
                return
   
            outputPath = self.workdir + '/' + self._targetFileBase + '_' + str(self._outputNumber) + self._targetFileSuffix
        
            if os.path.exists(outputPath):
                if not merger.AddFile(outputPath):
                    self.log('Cannot append to', outputPath)
                    return

                self.log('Appending to', outputPath)

            totalSize = 0
            toAdd = []
            while True:
                try:
                    inputPath = self.workdir + '/input/' + self.inputQueue.get(block = False) # will raise Queue.Empty exception when empty
                    if not merger.AddFile(inputPath):
                        self.log('Cannot add', inputPath, 'to list')
                        self.failed.append(inputPath)
                        continue
                    
                    toAdd.append(inputPath)
                    totalSize += os.stat(inputPath).st_size
                    if self._maxSize != 0 and totalSize / 1048576 >= self._maxSize: break
                    
                except Queue.Empty:
                    # loop over inputPaths reached the end -> no need to increment outputNumber
                    self._outputNumber -= 1
                    break

            self.log('hadd', tmpOutputPath, string.join(toAdd))

            if merger.Merge():
                self.log('mv', tmpOutputPath, outputPath)
                os.rename(tmpOutputPath, outputPath)
                self.succeeded += toAdd
            else:
                self.log('Merge failed')
                self.failed += toAdd
    
            self._outputNumber += 1
    

class HEfficiencyAdder(Hadder):
    """
    Hadder with efficiency calculation.
    """

    def __init__(self, name_, targetFileName_, maxSize = 0, workdir = ''):
        Hadder.__init__(self, name_, targetFileName_, maxSize, workdir)

    def finalize(self):
        if self._outputNumber != 0:
            self.log('Output seems to be a tree file. Will not calculate efficiency.')

        Reducer.finalize(self)

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
        print 'Usage: python reducer.py REDUCER WORKDIR [MAXSIZE]'
        sys.exit(1)

    workdir = sys.argv[2]

    try:
        maxSize = int(sys.argv[3])
    except IndexError:
        maxSize = 0

    sys.stdout.write('Output file name?: ')
    sys.stdout.flush()
    targetFileName = sys.stdin.readline().strip()

    reducer = eval(sys.argv[1])('reducer', targetFileName, maxSize, workdir)

    for file in os.listdir(workdir + '/input'):
        reducer.inputQueue.put(file)

    reducer.reduce()
    reducer.finalize()
