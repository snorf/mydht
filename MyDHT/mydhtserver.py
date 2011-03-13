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
import threading
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
        self.dht_table = MyDHTTable()
        self.client = MyDHTClient()
        self.ring_lock = threading.RLock()
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
        self.is_process = is_process
        if verbose:
            self.verbose = True
            self.client.verbose = True
        if logfile:
            sys.stdout = open(logfile,"w")
        self.serve()

    def add_new_node(self,new_node):
        """ Adds a new server to all existing nodes and
            sends the current ring back to it.
        """
        self.ring_lock.acquire()
        self.debug("adding:",new_node)
        host, port = new_node.split(":")
        newserver = Server(host,port)

        # Add the new server to all existing nodes
        for server in self.hash_ring.get_nodelist():
            if self.this_server != server:
                command = DHTCommand(DHTCommand.ADDNODE,newserver)
                self.client.sendcommand(server,command)
        # Convert to a string list
        ring = map(lambda serv: str(serv),set(self.hash_ring.ring.values()))
        # Add new server to this ring
        self.hash_ring.add_node(newserver)
        # Return |-separated list of nodes
        self.ring_lock.release()
        return "|".join(ring)

    def remove_node(self,node,replicated):
        """ Remove `node` from ring
            If not `replicated` tell all other nodes
            This usually happens if a node dies without being
            able to do a decommission.
        """
        self.ring_lock.acquire()
        self.hash_ring.remove_node(node)

        if not replicated:
            # First remove this server from all other servers
            # Add the new server to all existing nodes
            for server in self.hash_ring.get_nodelist():
                if self.this_server != server:
                    command = DHTCommand(DHTCommand.REMOVE,node)
                    command.replicated = True
                    self.client.sendcommand(server,command)

            rebalance_all_nodes()

        self.ring_lock.release()
        return "REMOVE ok"

    def decommission(self):
        """ Remove self from hash_ring and move all existing data
            to new node.
        """
        self.ring_lock.acquire()
        self.hash_ring.remove_node(self.this_server)

        # First remove this server from all other servers
        # Add the new server to all existing nodes
        for server in self.hash_ring.get_nodelist():
            if self.this_server != server:
                command = DHTCommand(DHTCommand.LEAVE,self.this_server)
                self.client.sendcommand(server,command)
        self.load_balance()
        self.ring_lock.release()

    def rebalance_all_nodes(self):
        # Load balance this node
        self.load_balance()

        # And load balance the others
        for server in self.hash_ring.get_nodelist():
            if self.this_server != server:
                command = DHTCommand(DHTCommand.BALANCE,node)
                self.client.sendcommand(server,command)

    def load_balance(self):
        """ Go through all keys in map and check with all replica server
            if they have the key, or else send it to them.
        """
        
        # For all keys in this map
        for key in self.dht_table.get_keys():
            self.debug("moving: ",key)
            key_is_at = self.hash_ring.get_replicas(key,self.this_server)
            for server in key_is_at:
                command = DHTCommand(DHTCommand.HASKEY,key)
                status = self.client.sendcommand(server,command)
                if status == "False":
                    # Key is missing in destination server, move it
                    value = self.dht_table.perform(DHTCommand(DHTCommand.GET,key))
                    command = DHTCommand(DHTCommand.PUT,key,value)
                    status = self.client.sendcommand(server,command)
                    self.debug(status)
        return "BALANCE ok"

    def purge(self):
        """ Go through all keys on this node and remove data
            that don't belong here.

            This could happen after a load balance where this
            node has moved data to another new responsible server.
        """
        # For all keys in this map
        for key in self.dht_table.get_keys():
            key_is_at = self.hash_ring.get_replicas(key)
            if self.this_server not in key_is_at:
                # Remove from map
                self.dht_table.perform(DHTCommand(DHTCommand.DEL,key))
        return "PURGE ok"

    def forward_data(self,from_socket,to_socket,length):
        """ Forwards data from `from_socket` to `to_socket`
            by using select to wait.
            `length` is the size.
        """
        done = 0
        buf = bytearray(_block)
        while done < length:
            readable, writable, exceptional = select.select([from_socket],[to_socket],[])
            if not readable: select.select([from_socket],[],[])
            elif not writable: select.select([],[to_socket],[])
            no_bytes = from_socket.recv_into(buf,_block)
            done += no_bytes
            sent = to_socket.send(buf[:no_bytes])
            assert sent == no_bytes

    def handle_replica_command(self,cmd,client_sock):
        """ Handle `cmd` from `client_sock`
            If the key is found on this server it will be handled here
            or else it will be forwarded to the first server that is
            responding.

            If it is a `DHTCommand.GET` and the key is found locally
            no other servers will be contacted.
        """
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
            # If the command is PUT, download the data before replicating
            if cmd.command == DHTCommand.PUT:
                received = 0
                data = StringIO()
                while received < cmd.size:
                    incoming = client_sock.recv(cmd.size - received)
                    if not incoming: break
                    data.write(incoming)
                    received += len(incoming)
                cmd.value = data.getvalue()
            status = self.dht_table.perform(cmd)
            # Replicate data to other nodes if it was not a GET command
            if not cmd.replicated:
                cmd.replicated = True
                if cmd.command != DHTCommand.GET:
                    for server in key_is_at:
                        remote_status = self.client.sendcommand(server,copy.deepcopy(cmd))
                        self.debug(remote_status)
        else:
            # Forward the request to the correct server
            # After one server has received the message break
            # one of the "correct" servers will take care of replication
            try:
                # For all possible replica nodes
                for server in key_is_at:
                    # Connect to remote node
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect((server.bindaddress()))

                    # Send the command
                    sock.send(cmd.getmessage())

                    # Forward data from incoming clientsock to sock
                    self.forward_data(client_sock,sock,cmd.size)

                    # Receive length of answer
                    length = sock.recv(_block)
                    client_sock.send(length)
                    length = int(length.split("|")[0])

                    # Forward the answer from sock to clientsock
                    self.forward_data(sock,client_sock,length)

                    # Close outgoing socket
                    sock.close()

                    # Close socket in a friendly way
                    client_sock.shutdown(SHUT_WR)
                    end = client_sock.recv(_block)
                    client_sock.close()
                    break
                else:
                    self.debug("No nodes were found")
                    status = "No nodes where found"
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
            if not cmd.replicated:
                # If the command was forwarded to another
                # server we have already closed the socket
                return
        elif cmd.command == DHTCommand.JOIN:
            # A client wants to join the ring
            status = self.add_new_node(cmd.key)
        elif cmd.command == DHTCommand.ADDNODE:
            # A new client has joined and should be added to this servers ring
            self.hash_ring.add_node(cmd.key)
            status = "added by "+str(self.this_server)
        elif cmd.command == DHTCommand.LEAVE:
            self.hash_ring.remove_node(cmd.key)
            status = "removed: "+str(cmd.key)
        elif cmd.command == DHTCommand.REMOVE:
            # A server has left the ring without decommission
            status = self.remove_node(cmd.key,cmd.replicated)
        elif cmd.command == DHTCommand.WHEREIS:
            # Just return the hostname that holds a key
            status = str(self.hash_ring.get_node(cmd.key))
        elif cmd.command == DHTCommand.PURGE:
            # Remove all data on this node that don't belong here
            status = self.purge()
        elif cmd.command == DHTCommand.BALANCE:
            # Load balance this node
            status = self.load_balance()
        elif cmd.command == DHTCommand.UNKNOWN:
            if command.startswith("GET /shutdown"):
                self.decommission()
                status = "SHUTDOWN ok"
            # Just send error and close socket
            status = "UNKNOWN_COMMAND"
            clientsock.send(status)
            clientsock.close()
            return
        else:
            # All other commands ends up in the table
            status = self.dht_table.perform(cmd)

        # Do not send the length to a web browser
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
        self.decommission()
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
            self.help()

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
        self.dht_table.set_hash_ring(self.hash_ring)

        self.debug("Starting server at",str(self.this_server))
        server_sock = socket(AF_INET,SOCK_STREAM)
        try:
            server_sock.bind((self.this_server.bindaddress()))
            server_sock.listen(5)
            while 1:
                client_sock, client_addr = server_sock.accept()
                #self.debug("Server connected by", clientaddr)
                thread.start_new_thread(self.serverthread, (client_sock,))
        except error, msg:
            print "Unable to bind to socket: ",msg

if __name__ == "__main__":
    MyDHT().cmdlinestart()
