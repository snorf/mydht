from _socket import *
import sys
from cmdapp import CmdApp

__author__ = 'Johan'
_block = 1024

class Client(CmdApp):
    def __init__(self):
        """A MyDHT client for interacting with MyDHT servers
        """
        CmdApp.__init__(self)
        self.port = int(self.getarg("port",50140))
        self.host = self.getarg("hostname","localhost")
        self.operator = self.getarg('-o') or self.getarg('-operator')
        self.value = self.getarg("-v") or self.getarg("-value")
        self.key = self.getarg("-k") or self.getarg("-key")
        self.sock = None
        self.debug("command:",\
        self.host,self.port,self.operator,self.key,self.value)

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
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.sock.send(self.operator + "\n")
        self.sock.send(self.key + "\n")

        if self.operator == "put":
            self.sock.send(self.value + "\n")

        while(1):
            data = self.sock.recv(_block)
            if not data: break
            self.debug("response from server",data)

        self.sock.close()
        self.debug("closed connection to server")

if __name__ == "__main__":
    Client().client()