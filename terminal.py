import sys
import subprocess
import signal
import time

DEBUG = False
            
class Terminal:
    """
    A wrapper for an ssh session.
    """

    def __init__(self, servName_):
        self._servName = servName_
        self._session = None
        self.node = ''
        self.open()

    def __del__(self):
        self.close(force = True)

    def open(self):
        if self.isOpen(): return
        
        self._session = subprocess.Popen(['ssh', '-oStrictHostKeyChecking=no', '-oLogLevel=quiet', '-T', self._servName],
            stdin = subprocess.PIPE,
            stdout = subprocess.PIPE,
            stderr = subprocess.STDOUT,
	    preexec_fn = lambda : signal.signal(signal.SIGINT, signal.SIG_IGN))
        
        self.node = self.communicate('echo $HOSTNAME')[0]
        print 'Terminal opened on ' + self.node
        
    def close(self, force = False):
        if not self.isOpen(): return
        
        try:
            if force: self._session.terminate()
            else: self._session.stdin.write('exit\n')

            iTry = 0
            while iTry < 5 and self._session.poll() is None:
                time.sleep(1)
                iTry += 1

            if self._session.poll() is None:
                self._session.terminate()
                
            self.node = ''
        except OSError:
            pass
        except:
            print 'Failed to close SSH connection:', sys.exc_info()[0:2]

    def isOpen(self):
        return self._session and self._session.poll() is None

    def write(self, line_):
        try:
            self._session.stdin.write(line_.strip() + '\n')
        except:
            print 'Failed to write {0} to terminal'.format(line_.strip()), sys.exc_info()[0:2]
            self.close(True)
            self.open()

    def read(self):
        response = ''
        try:
            response = self._session.stdout.readline().strip()
        except:
            print 'Failed to read from terminal', sys.exc_info()[0:2]
            self.close(True)
            self.open()

        return response

    def communicate(self, inputs_):
        output = []
        if DEBUG: print 'communicate: ', inputs_
        try:
            if isinstance(inputs_, list):
                for line in inputs_:
                    self._session.stdin.write(line.strip() + '\n')
            elif isinstance(inputs_, str):
                self._session.stdin.write(inputs_.strip() + '\n')

            self._session.stdin.write('echo EOL\n')

            while True:
                line = self._session.stdout.readline().strip()
                if line == 'EOL' or self._session.poll() is not None: break
                output.append(line)
        except:
            print 'Communication with terminal failed: ', sys.exc_info()[0:2]
            self.close(True)
            self.open()

        return output


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print 'Usage: python terminal.py HOST'
        sys.exit(1)
        
    term = Terminal(sys.argv[1])

    while term.isOpen():
        sys.stdout.write(term.node + '$ ')
        sys.stdout.flush()
        response = term.communicate(sys.stdin.readline())
        for line in response:
            print line
