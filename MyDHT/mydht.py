import collections
import signal
from socket import *
import sys
import thread
from HashRing import HashRing, Server
from cmdapp import CmdApp
_block = 1024

class MyDHT(CmdApp):
    def __init__(self):
        """ Main class for the DHT server
        """
        CmdApp.__init__(self)
        self.map = {}
        self.remoteserver = None
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
        if logfile:
            sys.stdout = open(logfile,"w")
            sys.stderr = sys.stderr
        self.serve()

    def sendcommand(self,server,command,*values):
        """ Sends a `command` to another `server` in the ring
        """
        # Check if server is a list of servers
        if isinstance(server,collections.Iterable):
            fromserver, data = None, None
            for srv in server:
                try:
                    fromserver, data = self.sendcommand(srv,command,*values)
                    if command == "get":
                        break
                except:
                    # Server is down?
                    continue
            return fromserver, data

        self.debug("sending command to:", str(server), command, str(values))
        sock = socket(AF_INET, SOCK_STREAM)
        try:
            sock.connect((server.bindaddress()))
            sock.send(command + "\n")

            # Send value(s) to another server
            if values:
                for val in values:
                    sock.send(str(val) + "\n")

            data = []
            while(1):
                incoming = sock.recv(_block)
                if not incoming: break
                else: data.append(incoming)

            self.debug("response from server",data)

            sock.close()
            self.debug("closed connection to server")
            # Return the concatenated data
            return server, "".join(data)
        except:
            print "Error connecting to server"
            return server,None

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
            try:
                del self.map[key]
            except KeyError:
                return "ERR_VALUE_NOT_FOUND"
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
        command = sockfile.readline()[:-1]

        server = self.thisserver
        # Commands that always should end up on this server
        if command == "join":
            server = sockfile.readline()[:-1]
            status = self.addnewserver(server)
        elif command == "addnode":
            newserver = sockfile.readline()[:-1]
            self.hashring.add_node(newserver)
            status = "added"
        elif command in ["put","get","del"]:
            key = sockfile.readline()[:-1]
            key_is_at = self.hashring.get_nodelist(key)
            value = []
            if command == "put":
                value = sockfile.readline()[:-1]
                
            # Check if key is found locally            
            if(self.thisserver in key_is_at):
                status = self.handlecommand(command,key,value)
            else:
                # Forward the request to the correct server
                self.debug("forwarding",command,"to:",str(key_is_at))
                server, status = self.sendcommand(key_is_at,command,key,value)
        else:
            self.debug("Invalid command",command)
            status = "INVALID_COMMAND"

        # Check if status is one or many messages
        #self.debug("sending:",status)
        clientsock.send(status)

        clientsock.close()
        #self.debug("connection closed")

    def signal_handler(self,signal,frame):
        self.debug("Exiting from SIGINT")
        # Exit softly if this is a process
        if self.is_process:
            os._exit(0)
        else:
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
            server, ring = self.sendcommand(remoteserver,"join",self.thisserver)
            self.debug("got ring from server:",str(ring))
            # Convert |-separated list to Server-instances
            nodes =  map(lambda serv: Server(serv.split(":")[0],serv.split(":")[1]) ,ring.split("|"))
            # Initialize local hashring
            self.hashring = HashRing(nodes)
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