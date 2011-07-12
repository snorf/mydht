import copy
import logging
import select
import signal
from socket import *
from socket import error as socket_error
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
           -s, --server
             specify server to join existing ring
           -r, --replicas
             specify the number of replicas (only first server may specify replicas, 3 is default)
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
            replicas = self.getarg("-r") or self.getarg("--replicas", 3)
            replicas = int(replicas)
            # Start the server
            self.start(host=host,port=port,replicas=replicas,remote_server=remoteserver)
        except ValueError:
            self.help()

    def start(self,host,port,replicas,remote_server=None,is_process=False):
        """ Starts the server with `hostname`, `port`
            If `remove_server` is not None it will be contacted to join an existing ring
            `is_process` is used when starting servers as processes, it is used to exit
            in a way pyunit likes if True.
        """
        self.this_server = Server(host,port)
        self.remote_server = remote_server
        self.replicas = replicas
        self.is_process = is_process
        self.serve()

    def add_new_node(self,new_node):
        """ Adds a new server to all existing nodes and
            returns a |-separated list of the current ring
            ","  the number of replicas used.
            Example:
            localhost:50140|localhost:50141,3
        """
        self.ring_lock.acquire()
        logging.debug("adding: %s", new_node)
        host, port = new_node.split(":")
        newserver = Server(host,port)
        self.hash_ring.remove_node(newserver)

        # Add the new server to all existing nodes
        command = DHTCommand(DHTCommand.ADDNODE,newserver)
        self.forward_command(command)

        # Convert to a string list
        ring = map(lambda serv: str(serv),self.hash_ring.get_nodelist())
        # Add new server to this ring
        self.hash_ring.add_node(newserver)
        # Return |-separated list of nodes
        self.ring_lock.release()
        return "|".join(ring)+","+str(self.hash_ring.replicas)

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
            self.load_balance(False)

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
                if not status:
                    # None was returned, servers is probably down
                    logging.error("Got no timestamp from %s, could be dead", str(server))
                    continue
                remote_time = float(status)
                local_timestamp = float(self.dht_table.perform(command))
                if remote_time < local_timestamp:
                    # Key is missing or old
                    logging.debug("Copying: %s to %s", key, server)
                    value = self.dht_table.perform(DHTCommand(DHTCommand.GET,key))
                    command = DHTCommand(DHTCommand.PUT,key,value,local_timestamp)
                    status = self.client.sendcommand(server,command)
                    logging.debug("status: %s", status)
                elif remote_time > local_timestamp:
                    # Remote object is newer, get it
                    logging.debug("Copying: %s from %s", key, server)
                    command = DHTCommand(DHTCommand.GET,key)
                    value = self.client.sendcommand(server,command)
                    status = self.dht_table.perform(DHTCommand(DHTCommand.PUT,key,value,remote_time))
                    logging.debug(status)
        return "BALANCE ok"

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
                    logging.debug(remote_status)
        return command

    def handle_replica_command(self,command,client_sock):
        """ Handle `command` from `client_sock`
            If the key is found on this server it will be handled here
            or else it will be forwarded to the first server that is
            responding.

            If it is a `DHTCommand.GET` and the key is found locally
            no other servers will be contacted.
        """
        # Find out where the key is found
        key_is_at = self.hash_ring.get_replicas(command.key)
        replica_servers = " ".join(map(lambda msg: str(msg), key_is_at))
        logging.debug("%s is at [%s] according to [%s]", command.key, replica_servers, str(self.hash_ring))

        # If key is local, remove this server from replicas
        local = (self.this_server in key_is_at)
        if local:
            key_is_at.remove(self.this_server)

        # If the command is PUT, download the data
        if command.action == DHTCommand.PUT:
            command.value = self.client.read_from_socket(command.size,client_sock)

        # If key is on this server, perform the action, if it was a get return immediately
        if local:
            status = self.dht_table.perform(command)
            if command.action == DHTCommand.GET and status != "ERR_VALUE_NOT_FOUND":
                return status

        # Replicate data to other nodes if it was not a GET command and the command was not already forwarded
        failures = 0
        if not command.forwarded:
            command.forwarded = True
            for server in key_is_at:
                response = self.client.sendcommand(server,copy.deepcopy(command))
                if response:
                    status = response
                    
                    if command.action != DHTCommand.GET:
                        logging.debug("remote status: %s", status)

                    # If this was a get and the response is valid, return
                    if command.action == DHTCommand.GET  and status != "ERR_VALUE_NOT_FOUND":
                        return status
                else:
                    failures += 1

        logging.debug("Performed command %s (failures: %d)", command, failures)
        return status

    def server_thread(self,client_sock):
        """ Thread that handles a client
            `client_sock` is the socket where the client is connected
            perform the operation and connect to another server if necessary
        """
        rawcommand = self.client.read_from_socket(_block,client_sock)
        command = DHTCommand().parse(rawcommand)

        logging.debug("received command: %s", str(command))
        if command.action in [DHTCommand.PUT,DHTCommand.GET,DHTCommand.DEL]:
            # Perform the command and any replication
            status = self.handle_replica_command(command,client_sock)
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
            # Just return the hostnames that holds a key
            status = ", ".join(map(lambda s: str(s), self.hash_ring.get_replicas(command.key)))
        elif command.action == DHTCommand.BALANCE:
            # Load balance this node
            status = self.load_balance(command.forwarded)
        elif command.action == DHTCommand.UNKNOWN:
            # Just send error and close socket
            status = "UNKNOWN_COMMAND"
            client_sock.send(status)
            client_sock.close()
            return
        else:
            # All other commands ends up in the table
            status = self.dht_table.perform(command)

        # Send length to all clients except a web browser (it will end up in the HTML)
        if command.action != DHTCommand.HTTPGET and command.action != DHTCommand.HTTPGETKEY:
            self.client.send_length_to_socket(len(status),client_sock)

        # Send response to client
        self.client.send_to_socket(status,len(status),client_sock)

        # Shutdown write end of socket
        client_sock.shutdown(SHUT_WR)
        # Close socket
        client_sock.close()

    def signal_handler(self,signal,frame):
        """ Handle SIGINT by doing decommission.
        """
        logging.debug("Exiting from SIGINT")
        self.decommission()
        if self.is_process:
            # Exit softly if this is a subprocess
            os._exit(0)
        else:
            # Exit hard if standalone
            sys.exit(0)

    def initialise_hashring(self):
        """ Initialize the hash ring.
            If `self.remote_server` is not note, get it from remote_server
            or else just create a new one.
            Also set the hash_ring in `self.dht_table`
        """
        if self.remote_server:
            remote_host, remote_port = self.remote_server.split(":")
            remote_server = Server(remote_host,remote_port)
            # Send a join command to the existing server
            command = DHTCommand(DHTCommand.JOIN,self.this_server)
            ring = self.client.sendcommand(remote_server,command)
            logging.debug("got ring from server: %s", str(ring))
            # Get replicas
            if not ring:
                raise RuntimeError(("Could not reach server: %s" % str(remote_server)))
            nodes,replicas = ring.split(",")
            # Convert |-separated list to Server-instances
            nodes =  map(lambda server_string: Server(server_string.split(":")[0],server_string.split(":")[1]) ,nodes.split("|"))
            # Initialize local hash ring
            self.hash_ring = HashRing(nodes,int(replicas))
            self.hash_ring.add_node(self.this_server)
        else:
            # First server so this server is added
            self.hash_ring = HashRing([self.this_server],self.replicas)

        # Initialize the hash map
        self.dht_table =  MyDHTTable(self.this_server,self.hash_ring)

    def serve(self):
        """ Main server process
            Starts a new `server_thread` for new clients
        """
        if not self.this_server:
            self.help()

        # Register SIGINT handler
        signal.signal(signal.SIGINT, self.signal_handler)

        logging.info("Starting server at %s", str(self.this_server))
        server_sock = socket(AF_INET,SOCK_STREAM)
        try:
            server_sock.bind((self.this_server.bindaddress()))
            server_sock.listen(5)

            # Get hash ring, we want to do this after we know that
            # the socket was free
            self.initialise_hashring()

            while 1:
                client_sock, client_addr = server_sock.accept()
                thread.start_new_thread(self.server_thread, (client_sock,))
        except socket_error:
            errno, errstr = sys.exc_info()[:2]
            logging.error("Unable to bind to socket: %s", errstr)
        except RuntimeError, e:
            logging.error("%s", e)

if __name__ == "__main__":
    MyDHTServer().cmdlinestart()
