import os
import sys
import time
from HashRing import HashRing

class CmdApp:
    """Base class for command line application.

    saves sys.argv, os.environ and if the user wants verbose output.
    getenv, getopt and getarg may be used to get command line parameters.
    """
    def __init__(self,name=None):
        self.name = name or self.__class__.__name__
        self.args = sys.argv[1:]
        self.env = os.environ
        self.verbose = self.getopt('-v') or self.getenv('VERBOSE')
        self.streams = sys.stdin, sys.stdout
        logfile = self.getarg("-l") or self.getarg("--logfile")
        if logfile:
            sys.stdout = open(logfile,"w")
        self.usage = "extended in subclass"

    def __del__(self):
        sys.stdin, sys.stdout = self.streams

    def getenv(self,name,default=None):
        """ Gets `name` from environment, if not found `default` is returned
        """
        try:
            return self.env[name]
        except KeyError:
            return default


    def getopt(self,tag):
        """ Returns 1 if `tag` is found in self.args or else returns 0
        """
        try:
            self.args.remove(tag)
            return 1
        except ValueError:
            return 0

    def getarg(self, tag, default=None):
        """ Gets the value of `tag` from command line parameters
            Example: "-x arg" returns arg when getarg("x") is called
        """
        try:
            pos = self.args.index(tag)
            val = self.args[pos+1]
            self.args[pos:pos+2] = []
            return val
        except Exception:
            return default

    def help(self):
        print "Usage:",self.name,"[options]",self.usage,
        print """  -v
             verbose mode
        """
        sys.exit(1)

    def now(self): return time.ctime(time.time())

    def debug(self,*message):
        if self.verbose:
            print "["+self.now()+"]:",message