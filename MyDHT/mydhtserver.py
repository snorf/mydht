from Queue import Queue, Empty
import cmd
import collections
import copy
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
        self.remote_server = None
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

    def start(self,host,port,remote_server=None,verbose=None,logfile=None,is_process=False):
        self.this_server = Server(host,port)
        self.remote_server = remote_server
        self.is_process = thread
        if verbose:
            self.verbose = True
            self.client.verbose = True
        if logfile:
            sys.stdout = open(logfile,"w")
        self.serve()

    def add_new_server(self,new_server_host_port):
        """ Adds a new server to all existing nodes and
            sends the current ring back to it.
        """
        self.debug("adding:",new_server_host_port)
        host, port = new_server_host_port.split(":")
        newserver = Server(host,port)

        # Add the new server to all existing nodes
        for server in self.hash_ring.ring.values():
            if self.this_server != server:
                command = DHTCommand(DHTCommand.ADDNODE,newserver)
                self.client.sendcommand(server,command)
        # Convert to a string list
        ring = map(lambda serv: str(serv),set(self.hash_ring.ring.values()))
        # Add new server to this ring
        self.hash_ring.add_node(newserver)
        # Return |-separated list of nodes
        return "|".join(ring)

    def handle_replica_command(self,cmd,clientsock):
        # Find out where the key is found
        key_is_at = self.hash_ring.get_replicas(cmd.key)
        self.debug(cmd.key,"is at",str(key_is_at),"according to",str(self.hash_ring))

        status = "UNKNOWN_ERROR"

        # Check if key is found locally
        if self.this_server in key_is_at:
            local = True
            key_is_at.remove(self.this_server)
        else:
            local = False

        if local:
            # If the command is PUT, download the data before
            # replicating
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
            # Replicate data to other nodes if it was not a GET command
            if not cmd.replicate:
                cmd.replicate = True
                if cmd.command != DHTCommand.GET:
                    for server in key_is_at:
                        remote_status = self.client.sendcommand(server,copy.deepcopy(cmd))
                        self.debug(remote_status)
        else:
            # Forward the request to the correct server
            # In short use select to wait for the incoming and outgoing
            # socket and the relay the message
            # After one server has received the message break
            # one of the "correct" servers will take care of replication
            try:
                # For all possible replica nodes
                for server in key_is_at:
                    sock = socket(AF_INET, SOCK_STREAM)
                    
                    sock.connect((server.bindaddress()))
                    # If value send the command and the size of value
                    sock.send(cmd.getmessage())

                    received = 0
                    buf = bytearray(_block)
                    while received < cmd.size:
                        r, w, e = select.select([clientsock],[sock],[])
                        if not r: select.select([clientsock],[],[])
                        elif not w: select.select([],[sock],[])
                        no_bytes = clientsock.recv_into(buf,_block)
                        received += no_bytes
                        sent = sock.sendall(buf[:no_bytes])

                    length = sock.recv(_block)
                    clientsock.send(length)
                    length = int(length.split("|")[0])

                    received = 0
                    while received < length:
                        r, w, e = select.select([sock],[clientsock],[])
                        if not r: select.select([sock],[],[])
                        elif not w: select.select([],[clientsock],[])
                        no_bytes = sock.recv_into(buf,_block)
                        received += no_bytes
                        clientsock.sendall(buf[:no_bytes])
                    sock.close()
                    clientsock.shutdown(SHUT_WR)
                    end = clientsock.recv(_block)
                    clientsock.close()
                    break
                else:
                    self.debug("No nodes were found")
            except:
                print "Error relaying to server: ", server
                print '-'*60
                traceback.print_exc()
                print '-'*60
        return status

    def serverthread(self,clientsock):
        """ Thread that handles a client
            `clientsock` is the socket where the client is connected
            perform the operation and connect to another server if necessary
        """
        command = clientsock.recv(_block)
        cmd = DHTCommand().parse(command)

        self.debug("received",str(cmd))
        if cmd.command in [DHTCommand.PUT,DHTCommand.GET,DHTCommand.DEL]:
            status = self.handle_replica_command(cmd,clientsock)
            if not cmd.replicate:
                return
        elif cmd.command == DHTCommand.JOIN:
            # A client wants to join the ring
            status = self.add_new_server(cmd.key)
        elif cmd.command == DHTCommand.ADDNODE:
            # A new client has joined and should be added to this servers ring
            self.hash_ring.add_node(cmd.key)
            status = "added by "+str(self.this_server)
        elif cmd.command == DHTCommand.WHEREIS:
            # Just return the hostname that holds a key
            status = str(self.hash_ring.get_node(cmd.key))
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
        if not self.this_server:
            self.usage()

        # Register SIGINT handler
        signal.signal(signal.SIGINT, self.signal_handler)

        if self.remote_server:
            remote_host, remote_port = self.remote_server.split(":")
            remote_server = Server(remote_host,remote_port)
            # Send a join command to the existing server
            command = DHTCommand(DHTCommand.JOIN,self.this_server)
            ring = self.client.sendcommand(remote_server,command)
            self.debug("got ring from server:",str(ring))
            # Convert |-separated list to Server-instances
            nodes =  map(lambda server_string: Server(server_string.split(":")[0],server_string.split(":")[1]) ,ring.split("|"))
            # Initialize local hash ring
            self.hash_ring = HashRing(nodes)
            self.hash_ring.add_node(self.this_server)
        else:
            # First server so this server is added
            self.hash_ring = HashRing([self.this_server])

        self.debug("Starting server at",str(self.this_server))
        server_sock = socket(AF_INET,SOCK_STREAM)
        try:
            server_sock.bind((self.this_server.bindaddress()))
            server_sock.listen(5)
            while(1):
                client_sock, client_addr = server_sock.accept()
                #self.debug("Server connected by", clientaddr)
                thread.start_new_thread(self.serverthread, (client_sock,))
        except error, msg:
            print "Unable to bind to socket: ",msg

if __name__ == "__main__":
    MyDHT().cmdlinestart()
