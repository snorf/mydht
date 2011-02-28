from socket import *
import thread
from cmdapp import CmdApp

class MyDHT(CmdApp):
    def __init__(self):
        """
        """
        CmdApp.__init__(self)
        self.port = int(self.getarg("port",50140))
        self.host = self.getarg("hostname","localhost")
        self.map = {}

    def insert(self,key,value):
        self.debug("inserting",key)
        self.map[key] = value

    def get(self,key):
        self.debug("getting",key)
        try:
            return self.map[key]
        except KeyError:
            return ""

    def remove(self,key):
        self.debug("removing",key)
        del self.map[key]

    def serverthread(self,clientsock):
        sockfile = clientsock.makefile('r') # wrap socket in dup file obj
        command = sockfile.readline()[:-1]
        key = sockfile.readline()[:-1]
        self.debug("Command received:",command,"key:",key)
        if command == "put":
            value = sockfile.readline()[:-1]
            self.insert(key,value)
            clientsock.send("OK")
        elif command == "get":
            value = self.get(key)
            clientsock.send(value)
        elif command == "del":
            self.remove(key)
            clientsock.send("OK")
        clientsock.close()
        self.debug("connection closed")

    def server(self):
        self.debug("Starting server at",self.host,self.port)
        serversock = socket(AF_INET,SOCK_STREAM)
        serversock.bind((self.host,self.port))
        serversock.listen(5)
        while(1):
            clientsock, clientaddr = serversock.accept()
            self.debug("Server connected by", clientaddr)
            thread.start_new_thread(self.serverthread, (clientsock,))


if __name__ == "__main__":
    MyDHT().server()