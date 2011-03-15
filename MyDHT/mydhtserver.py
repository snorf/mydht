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

_block = 4096

class MyDHTServer(CmdApp):
    def __init__(self):
        """ Main class for the DHT server
        """
        CmdApp.__init__(self)
        self.remote_server = None
        self.dht_table = None
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
        """ Starts the server with `hostname`, `port`
            If `remove_server` is not None it will be contacted to join an existing ring
            `verbose` enables debug logging
            `logfile` specifies a logfile instead of stdout
            `is_process` is used when starting servers as processes, it is used to exit
            in a way pyunit likes if True.
        """
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
            returns a |-separated list of the current ring.
        """
        self.ring_lock.acquire()
        self.debug("adding:",new_node)
        host, port = new_node.split(":")
        newserver = Server(host,port)

        # Add the new server to all existing nodes
        command = DHTCommand(DHTCommand.ADDNODE,newserver)
        self.forward_command(command)

        # Convert to a string list
        ring = map(lambda serv: str(serv),set(self.hash_ring.ring.values()))
        # Add new server to this ring
        self.hash_ring.add_node(newserver)
        # Return |-separated list of nodes
        self.ring_lock.release()
        return "|".join(ring)

    def remove_node(self,node,forwarded):
        """ Remove `node` from ring
            If not `forwarder` tell all other nodes
            This usually happens if a node dies without being
            able to do a decommission.
        """
        self.ring_lock.acquire()
        self.hash_ring.remove_node(node)

        if not forwarded:
            # Add the new server to all existing nodes
            command = DHTCommand(DHTCommand.REMOVE,node)
            self.forward_command(command)

            # Rebalance all nodes
            rebalance_all_nodes()

        self.ring_lock.release()
        return "REMOVE ok"

    def decommission(self):
        """ Remove self from hash_ring and move all existing data
            to new nodes.
        """
        self.ring_lock.acquire()
        self.hash_ring.remove_node(self.this_server)

        # First remove this server from all other servers
        command = DHTCommand(DHTCommand.LEAVE,self.this_server)
        self.forward_command(command)

        # Load balance only on this node
        self.load_balance(True)
        self.ring_lock.release()

    def load_balance(self,forwarded):
        """ Load balance this node and then all other.
            if `forwarded` is False BALANCE will be sent to
            all other nodes.
        """
        status = self.internal_load_balance()

        if not forwarded:
            # And load balance the others
            command = DHTCommand(DHTCommand.BALANCE)            
            self.forward_command(command)

        return status
    
    def internal_load_balance(self):
        """ Go through all keys in map and check with all replica server
            if they have an older version of the key, send the value.
            if they have a newer version, get the value.
        """
        
        # For all keys in this map
        for key in self.dht_table.get_keys():
            key_is_at = self.hash_ring.get_replicas(key,self.this_server)
            for server in key_is_at:
                command = DHTCommand(DHTCommand.HASKEY,key)
                status = self.client.sendcommand(server,command)
                remote_time = float(status)
                local_timestamp = float(self.dht_table.perform(command))
                if remote_time < local_timestamp:
                    # Key is missing or old
                    self.debug("Copying: ",key,"to",server)
                    value = self.dht_table.perform(DHTCommand(DHTCommand.GET,key))
                    command = DHTCommand(DHTCommand.PUT,key,value,local_timestamp)
                    status = self.client.sendcommand(server,command)
                    self.debug(status)
                elif remote_time > local_timestamp:
                    # Remote object is newer, get it
                    self.debug("Copying: ",key,"from",server)
                    command = DHTCommand(DHTCommand.GET,key)
                    value = self.client.sendcommand(server,command)
                    status = self.dht_table.perform(DHTCommand(DHTCommand.PUT,key,value,remote_time))
                    self.debug(status)
        return "BALANCE ok"

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

    def forward_command(self,command):
        """ Forwards `command` to all other servers except this
            Sets command.forwarded to True so that the receiving
            nodes does not forward the command.
        """
        if not command.forwarded:
            command.forwarded = True
            for server in self.hash_ring.get_nodelist():
                if self.this_server != server:
                    remote_status = self.client.sendcommand(server,copy.deepcopy(command))
                    self.debug(remote_status)
        return command

    def handle_replica_command(self,command,client_sock):
        """ Handle `cmd` from `client_sock`
            If the key is found on this server it will be handled here
            or else it will be forwarded to the first server that is
            responding.

            If it is a `DHTCommand.GET` and the key is found locally
            no other servers will be contacted.
        """
        # Find out where the key is found
        key_is_at = self.hash_ring.get_replicas(command.key)
        self.debug(command.key,"is at",str(key_is_at),"according to",str(self.hash_ring))

        status = "UNKNOWN_ERROR"

        # Check if key is found locally
        if self.this_server in key_is_at:
            local = True
            key_is_at.remove(self.this_server)
        else:
            local = False

        if local:
            # If the command is PUT, download the data before replicating
            if command.action == DHTCommand.PUT:
                command.value = self.client.read_from_socket(command.size,client_sock)
                
            status = self.dht_table.perform(command)
            # Replicate data to other nodes if it was not a GET command
            if not command.forwarded:
                command.forwarded = True
                if command.action != DHTCommand.GET:
                    for server in key_is_at:
                        remote_status = self.client.sendcommand(server,copy.deepcopy(command))
                        self.debug(remote_status)

        else:
            # Forward request to one of the servers responsible for the key
            # The responding server will take care of replication
            for server in key_is_at:
                try:
                    # For all possible replica nodes
                    # Connect to remote node
                    sock = socket(AF_INET, SOCK_STREAM)
                    sock.connect((server.bindaddress()))

                    # Send the command
                    sock.send(command.getmessage())

                    # Forward data from incoming clientsock to sock
                    self.forward_data(client_sock,sock,command.size)

                    # Receive length of answer
                    length = self.client.read_length_from_socket(sock)
                    # Forward length to client
                    self.client.send_length_to_socket(length,client_sock)

                    # Forward the answer from sock to clientsock
                    self.forward_data(sock,client_sock,length)

                    # Close outgoing socket
                    sock.close()

                    # Close socket in a friendly way
                    client_sock.shutdown(SHUT_WR)
                    end = client_sock.recv(_block)
                    client_sock.close()
                    break
                except Exception, e:
                    print "Error relaying to server: ", server
                    print '-'*60
                    traceback.print_exc()
                    print '-'*60
            else:
                self.debug("No nodes were found")
                status = "No nodes where found"

        return status

    def server_thread(self,client_sock):
        """ Thread that handles a client
            `client_sock` is the socket where the client is connected
            perform the operation and connect to another server if necessary
        """
        rawcommand = client_sock.recv(_block)
        command = DHTCommand().parse(rawcommand)

        self.debug("received",str(command))
        if command.action in [DHTCommand.PUT,DHTCommand.GET,DHTCommand.DEL]:
            status = self.handle_replica_command(command,client_sock)
            # If the command was forwarded to another
            # server we have already closed the socket
            if not command.forwarded:
                return
        elif command.action == DHTCommand.JOIN:
            # A client wants to join the ring
            status = self.add_new_node(command.key)
        elif command.action == DHTCommand.ADDNODE:
            # A new client has joined and should be added to this servers ring
            self.hash_ring.add_node(command.key)
            status = "added by "+str(self.this_server)
        elif command.action == DHTCommand.LEAVE:
            self.hash_ring.remove_node(command.key)
            status = "removed: "+str(command.key)
        elif command.action == DHTCommand.REMOVE:
            # A server has left the ring without decommission
            status = self.remove_node(command.key,command.forwarded)
        elif command.action == DHTCommand.WHEREIS:
            # Just return the hostname that holds a key
            status = str(self.hash_ring.get_node(command.key))
        elif command.action == DHTCommand.BALANCE:
            # Load balance this node
            status = self.load_balance(command.forwarded)
        elif command.action == DHTCommand.UNKNOWN:
            if rawcommand.startswith("GET /shutdown"):
                self.decommission()
                status = "SHUTDOWN ok"
            # Just send error and close socket
            status = "UNKNOWN_COMMAND"
            client_sock.send(status)
            client_sock.close()
            return
        else:
            # All other commands ends up in the table
            status = self.dht_table.perform(command)

        # Send length to all clients except a web browser (it will end up in the HTML)
        if command.action != DHTCommand.HTTPGET:
            self.client.send_length_to_socket(len(status),client_sock)

        # Send response to client
        self.client.send_to_socket(status,len(status),client_sock)

        # Shutdown write end of socket
        client_sock.shutdown(SHUT_WR)
        # Wait for end which will be received when the client closes the socket.
        end = client_sock.recv(_block)
        # Close socket
        client_sock.close()

    def signal_handler(self,signal,frame):
        """ Handle SIGINT by doing decommission.
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
            Contact `remote_server` to join existing ring
            or create a new one (if `remote_server` is None).

            Starts a new `server_thread` for new clients
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

        # Initialize the hash map
        self.dht_table =  MyDHTTable(self.this_server,self.hash_ring)

        self.debug("Starting server at",str(self.this_server))
        server_sock = socket(AF_INET,SOCK_STREAM)
        try:
            server_sock.bind((self.this_server.bindaddress()))
            server_sock.listen(5)
            while 1:
                client_sock, client_addr = server_sock.accept()
                #self.debug("Server connected by", clientaddr)
                thread.start_new_thread(self.server_thread, (client_sock,))
        except error, msg:
            print "Unable to bind to socket: ",msg

if __name__ == "__main__":
    MyDHTServer().cmdlinestart()
