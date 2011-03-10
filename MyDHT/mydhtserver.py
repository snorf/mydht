import cmd
import collections
import select
import signal
from socket import *
import sys
import os
import thread
import traceback
from HashRing import HashRing, Server
from MyDHTTable import MyDHTTable
from cmdapp import CmdApp
from dhtcommand import DHTCommand
from mydhtclient import MyDHTClient
from cStringIO import StringIO

_block = 1024

class MyDHT(CmdApp):
    def __init__(self):
        """ Main class for the DHT server
        """
        CmdApp.__init__(self)
        self.remoteserver = None
        self.map = MyDHTTable()
        self.client = MyDHTClient()
        self.usage = \
        """
           -p, --port
             specify port (default: 50140)
           -h, --hostname
             specify hostname (default: localhost)
        """

    def cmdlinestart(self):
        """ Start server from cmdline
        get -p, --port and -h, --hostname parameters
        """
        try:
            port = int(self.getarg("-p") or self.getarg("--port",50140))
            host = self.getarg("-h") or self.getarg("--hostname","localhost")
            # Check if we should connect to existing ring
            remoteserver = self.getarg("-s") or self.getarg("-server")
            # Start the server
            self.start(host,port,remoteserver)
        except ValueError:
            self.help()

    def start(self,host,port,remoteserver,verbose=None,logfile=None,is_process=False):
        self.thisserver = Server(host,port)
        self.remoteserver = remoteserver
        self.is_process = thread
        if verbose:
            self.verbose = True
            self.client.verbose = True
        if logfile:
            sys.stdout = open(logfile,"w")
        self.serve()

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
                self.client.sendcommand(server,DHTCommand.ADDNODE,newserver)
        # Convert to a string list
        ring = map(lambda serv: str(serv),set(self.hashring.ring.values()))
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
        command = clientsock.recv(_block)
        cmd = DHTCommand().parse(command)

        self.debug("received",str(cmd))
        if cmd.command in [DHTCommand.PUT,DHTCommand.GET,DHTCommand.DEL]:
            key_is_at = self.hashring.get_node(cmd.key)
            self.debug(cmd.key,"is at",str(key_is_at),"according to",str(self.hashring))

            # Check if key is found locally
            if self.thisserver == key_is_at:
                if cmd.command == DHTCommand.PUT:
                    received = 0
                    data = StringIO()
                    while received < cmd.size:
                        incoming = clientsock.recv(cmd.size - received)
                        if not incoming: break
                        data.write(incoming)
                        received += len(incoming)
                    cmd.value = data.getvalue()
                status = self.map.perform(cmd)
            else:
                # Forward the request to the correct server
                #self.debug("sending command to:", str(server), str(command),"try number",retry)
                sock = socket(AF_INET, SOCK_STREAM)

                try:
                    sock.connect((key_is_at.bindaddress()))
                    # If value send the command and the size of value
                    sock.send(cmd.getmessage())

                    received = 0
                    buf = bytearray(_block)
                    while received < cmd.size:
                        r, w, e = select.select([clientsock],[sock],[])
                        if not r: select.select([clientsock],[],[])
                        elif not w: select.select([],[sock],[])
                        nobytes = clientsock.recv_into(buf,_block)
                        received += nobytes
                        sent = sock.sendall(buf[:nobytes])

                    length = sock.recv(_block)
                    clientsock.send(length)
                    length = int(length.split("|")[0])

                    received = 0
                    while received < length:
                        r, w, e = select.select([sock],[clientsock],[])
                        if not r: select.select([sock],[],[])
                        elif not w: select.select([],[clientsock],[])
                        nobytes = sock.recv_into(buf,_block)
                        received += nobytes
                        clientsock.sendall(buf[:nobytes])
                    sock.close()
                    clientsock.shutdown(SHUT_WR)
                    end = clientsock.recv(_block)
                    clientsock.close()
                    return
                except:
                    print "Error relaying to server:"
                    print '-'*60
                    traceback.print_exc()
                    print '-'*60
                status = "" #self.client.sendcommand(key_is_at,cmd)
        elif cmd.command == DHTCommand.JOIN:
            # A client wants to join the ring
            status = self.addnewserver(cmd.key)
        elif cmd.command == DHTCommand.ADDNODE:
            # A new client has joined and should be added to this servers ring
            self.hashring.add_node(cmd.key)
            status = "added by "+str(self.thisserver)
        elif cmd.command == DHTCommand.WHEREIS:
            # Just return the hostname that holds a key
            status = str(self.hashring.get_node(cmd.key))
        else:
            # All other commands ends up in the table
            status = self.map.perform(cmd)

        # Only send the length to those who understand it
        if cmd.command != DHTCommand.HTTPGET:
            length = str(len(status)) + "|"
            length = length + ("0"*(_block-len(length)))
            clientsock.send(length)
        clientsock.send(status)
        clientsock.shutdown(SHUT_WR)
        end = clientsock.recv(_block)
        clientsock.close()

    def signal_handler(self,signal,frame):
        """ Handle SIGINT
        """
        self.debug("Exiting from SIGINT")
        if self.is_process:
            # Exit softly if this is a subprocess
            os._exit(0)
        else:
            # Exit hard if standalone
            sys.exit(0)

    def serve(self):
        """ Main server process
            Starts a new thread for new clients
        """
        if not self.thisserver:
            self.usage()

        # Register SIGINT handler
        signal.signal(signal.SIGINT, self.signal_handler)

        if self.remoteserver:
            remotehost, remoteport = self.remoteserver.split(":")
            remoteserver = Server(remotehost,remoteport)
            # Send a join command to the existing server
            command = DHTCommand(DHTCommand.JOIN,self.thisserver)
            ring = self.client.sendcommand(remoteserver,command)
            self.debug("got ring from server:",str(ring))
            # Convert |-separated list to Server-instances
            nodes =  map(lambda serv: Server(serv.split(":")[0],serv.split(":")[1]) ,ring.split("|"))
            # Initialize local hashring
            self.hashring = HashRing(nodes)
            self.hashring.add_node(self.thisserver)
        else:
            # First server so this server is added
            self.hashring = HashRing([self.thisserver])

        self.debug("Starting server at",str(self.thisserver))
        serversock = socket(AF_INET,SOCK_STREAM)
        try:
            serversock.bind((self.thisserver.bindaddress()))
            serversock.listen(5)
            while(1):
                clientsock, clientaddr = serversock.accept()
                #self.debug("Server connected by", clientaddr)
                thread.start_new_thread(self.serverthread, (clientsock,))
        except error, msg:
            print "Unable to bind to socket: ",msg

if __name__ == "__main__":
    MyDHT().cmdlinestart()
