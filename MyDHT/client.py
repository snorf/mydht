from _socket import *
import sys
from mydhtclient import MyDHTClient
from HashRing import Server
from cmdapp import CmdApp

__author__ = 'Johan'
_block = 1024

class Client(CmdApp):
    def __init__(self):
        """A MyDHT client for interacting with MyDHT servers
        """
        CmdApp.__init__(self)
        self.sock = None
        self.remoteserver = None
        self.usage = \
        """
           -c, --command
             put, get, del
           -h, --hostname
             specify hostname (default: localhost)
        """

    def cmdlinestart(self):
        """ Parse command line parameters and start client
        """
        try:
            port = int( self.getarg("-p") or self.getarg("--port",50140))
            host = self.getarg("-h") or self.getarg("--hostname","localhost")
            self.key = self.getarg("-k") or self.getarg("-key")
            self.server = Server(host,port)
            self.command = self.getarg('-c') or self.getarg('-command')
            self.value = self.getarg("-val") or self.getarg("-value")
            self.debug("command:",\
            str(self.server),self.command,self.key,self.value)
            if self.command is None or self.key is None or self.server is None:
                self.help()
        except TypeError:
            self.help()
        self.client()

    def usage(self):
        print "Usage: -o operator -k key -v value"
        sys.exit(1)

    def put(self,file):
        """ Open self.file in binary mode and send it to socket
        """
        try:
            while(1):
                bytes = file.read(_block)
                if not bytes: break
                sent = self.sock.send(bytes)
                assert sent == len(bytes)
        except Exception:
            print "Error uploading:",self.file
            
    def client(self):
        server, response = MyDHTClient().sendcommand(self.server,self.command,self.key,self.value)
        print str(server),":",response

if __name__ == "__main__":
    Client().cmdlinestart()