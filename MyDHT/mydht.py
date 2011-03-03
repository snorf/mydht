from socket import *
import thread
from HashRing import HashRing, Server
from cmdapp import CmdApp
_block = 1024

class MyDHT(CmdApp):
    def __init__(self):
        """ Main class for the DHT server
        """
        CmdApp.__init__(self)
        port = int(self.getarg("-p") or self.getarg("port",50140))
        host = self.getarg("hostname","localhost")
        self.thisserver = Server(host,port)
        self.map = {}
        # Check if we should connect to existing ring
        self.remoteserver = self.getarg("-s") or self.getarg("-server")
        if self.remoteserver:
            remotehost, remoteport = self.remoteserver.split(":")
            remoteserver = Server(remotehost,remoteport)
            # Send a join command to the existing server
            ring = self.sendcommand(remoteserver,"join",self.thisserver)
            # Convert |-separated list to Server-instances
            nodes =  map(lambda serv: Server(serv.split(":")[0],serv.split(":")[1]) ,ring.split("|"))
            # Initialize local hashring
            self.hashring = HashRing(nodes)
        else:
            # First server so this server is added
            self.hashring = HashRing([self.thisserver])

    def sendcommand(self,server,command,*values):
        """ Sends a `command` to another `server` in the ring
        """
        self.debug("sending command to:", str(server), command, str(values))
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((server.host, server.port))
        sock.send(command + "\n")

        # Send value(s) to another server
        if values:
            for val in values:
                sock.send(str(val) + "\n")

        data = []
        while(1):
            incoming = sock.recv(_block)
            data.append(incoming)
            if not incoming: break
        self.debug("response from server",data)

        sock.close()
        self.debug("closed connection to server")
        # Return the concatenated data
        return "".join(data)

    def handlecommand(self,command,key,value):
        """ Handle `command` for `key` on this server
        """
        if command == "put":
            self.map[key] = value
        elif command == "get":
            try:
                return self.map[key]
            except KeyError:
                return "ERR_VALUE_NOT_FOUND"
        elif command == "del":
            del self.map[key]
        return "OK"

    def addnewserver(self,newserverhostport):
        """ Adds a new server to all existing nodes and
            sends the current ring back to it.
        """
        self.debug("adding:",newserverhostport)
        host, port = newserverhostport.split(":")
        newserver = Server(host,port)

        # Add the new server to all existing nodes
        for server in self.hashring.ring.values():
            if self.thisserver != server:
                self.sendcommand(server,"addnode",newserver)
        # Convert to a string list
        ring = map(lambda serv: str(serv),self.hashring.ring.values())
        # Add new server to this ring
        self.hashring.add_node(newserver)
        # Return |-separated list of nodes
        return "|".join(ring)

    def serverthread(self,clientsock):
        """ Thread that handles a client
            `clientsock` is the socket where the client is connected
            perform the operation and connect to another server if necessary
        """
        sockfile = clientsock.makefile('r') # wrap socket in dup file obj
        command = sockfile.readline()[:-1]
        self.debug("Command received:",command)

        status = "OK"
        # Commands that always should end up on this server
        if command == "join":
            server = sockfile.readline()[:-1]
            status = self.addnewserver(server)
        elif command == "newserver":
            newserver = sockfile.readline()[:-1]
            status = self.hashring.add_node(newserver)
        elif command == "addring":
            # Add all servers to ring
            while(1):
                newserver = self.sock.recv()[:-1]
                self.hashring.add_node(newserver)
                if not data: break
        elif command in ["put","get","del"]:
            key = sockfile.readline()[:-1]
            key_is_at = self.hashring.get_nodes(key)
            value = []
            if command == "put":
                while(1):
                    data = sockfile.readline(_block)
                    value.append(data)
                    if not data: break

            # Check if key is found locally            
            if(self.thisserver in key_is_at):
                status = self.handlecommand(command,key,value)
            else:
                # Forward the request to the correct server
                status = self.sendcommand(key_is_at,command,value)
        else:
            self.debug("Invalid command",command)
            status = "INVALID_COMMAND"

        # Check if status is one or many messages
        self.debug("sending:",status)
        clientsock.send(status)
        clientsock.send("\n")

        clientsock.close()
        self.debug("connection closed")

    def server(self):
        """ Main server process
            Starts a new thread for new clients
        """
        self.debug("Starting server at",str(self.thisserver))
        serversock = socket(AF_INET,SOCK_STREAM)
        serversock.bind((self.thisserver.bindaddress()))
        serversock.listen(5)
        while(1):
            clientsock, clientaddr = serversock.accept()
            self.debug("Server connected by", clientaddr)
            thread.start_new_thread(self.serverthread, (clientsock,))

if __name__ == "__main__":
    MyDHT().server()