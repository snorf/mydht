from _socket import *
import logging
from socket import error as socket_error
import sys
import traceback
from HashRing import Server
from cmdapp import CmdApp
from StringIO import StringIO
from dhtcommand import DHTCommand

__author__ = 'Johan'
_block = 4096

class MyDHTClient(CmdApp):
    def __init__(self,verbose=False,logfile=None):
        """A MyDHT client for interacting with MyDHT servers
        """
        CmdApp.__init__(self,verbose=verbose,logfile=logfile)
        self.usage = \
        """
           -h, --hostname
             specify hostname (default: localhost)
           -p, --port
             specify port (default: 50140)
           -c, --command
             put, get, del, haskey, purge, remove, whereis, balance
           -k, --key
             specify key
           -val, --value
             specify a (string) value
           -f, --file
             specify a file value
        """

    def send_to_socket(self,data,size,socket):
        """ Send `size` amount of `data` to `socket`
            If data is a str it will be converted to
            StringIO. If it is not an str it is assumed
            to be some kind of stream object (ie file).
        """
        if isinstance(data,str):
            # Copy string contents to StringIO object
            data = StringIO(data)
        totalsent = 0
        while totalsent < size:
            chunk = data.read(_block)
            if not chunk: break
            sent = socket.send(chunk)
            if not sent:
                raise RuntimeError("socket connection broken")
            totalsent += sent

    def read_from_socket(self,size,socket,outstream=None):
        """  Read `size` data from `socket` and save it
             to either outstream (if it is an open file)
             or return it as a string.

             To be able to handle web request there it is possible
             to break the loop prior to size has been received.
             It looks for a GET / in the beginning of the data
             and \r\n\r\n in the end.
        """
        received = 0
        data = StringIO()
        webbrowser = False
        while received < size:
            incoming = socket.recv(size - received)
            if not incoming: break
            received += len(incoming)
            if isinstance(outstream,file):
                # If outstream is a file, write to it
                outstream.write(incoming)
            else:
                data.write(incoming)

            # Check if incoming starts with HTTP GET
            if incoming[0:5] == "GET /":
                webbrowser = True
            # Break if data endswith \r\n and webbrowser
            if webbrowser and incoming.endswith("\r\n\r\n"):
                logging.debug("Request was a browser, breaking")
                break;

        return data.getvalue()

    def send_length_to_socket(self,length,socket):
        """ Create a new length packet and send it to `socket`
        """
        length = str(length) + "|"
        length = length + ("0"*(_block-len(length)))
        self.send_to_socket(length,_block,socket)

    def read_length_from_socket(self,socket):
        """ Read the length packet from a socket.
            The packet is length + | + zero padding.
            return it as an int.
        """
        length = self.read_from_socket(_block,socket)
        length = int(length.split("|")[0])
        return length

    def sendcommand(self,server,command,outstream=None):
        """ Sends a `command` to a `server` in the ring
            `outstream` is used when the client wants the output
            value written to an output stream.
        """

        # If command isn't already a DHTCommand, create one

        for retry in range(3):
            logging.debug("sending command to: %s %s try number: %d", str(server), str(command), retry)
            sock = socket(AF_INET, SOCK_STREAM)

            try:
                sock.connect((server.bindaddress()))
                # If value send the command and the size of value
                sock.send(command.getmessage())

                # Send value to another server
                if command.value:
                    self.send_to_socket(command.value,command.size,sock)

                length = self.read_length_from_socket(sock)
                data = self.read_from_socket(length,sock,outstream)

                sock.close()
                return data
            except socket_error:
                errno, errstr = sys.exc_info()[:2]
                logging.error("Error connecting to server: %s", errstr)

        logging.error("Server (%s) did not respond during 3 tries, giving up", str(server))
        return None


    def get_command(self, string):
        for i,command in DHTCommand().allcommands.iteritems():
            if command == string.upper():
                return i
        else:
            return 0

    def cmdlinestart(self):
        """ Parse command line parameters and start client
        """
        try:
            port = int( self.getarg("-p") or self.getarg("--port",50140))
            host = self.getarg("-h") or self.getarg("--hostname","localhost")
            key = self.getarg("-k") or self.getarg("--key")
            server = Server(host,port)
            command = self.getarg('-c') or self.getarg('--command')
            value = self.getarg("-val") or self.getarg("--value")
            file = self.getarg("-f") or self.getarg("--file")
            outfile = self.getarg("-o") or self.getarg("--outfile")

            logging.debug("command: %s %s %s %s", str(server), command, key, value)
            if command is None or server is None or file and value:
                self.help()
                
            command = self.get_command(command)
            if file:
                f = open(file, "rb")
                command = DHTCommand(command,key,f)
            else:
                command = DHTCommand(command,key,value)

            if outfile:
                # File was an argument, supply it
                with open(outfile, "wb") as out:
                    print self.sendcommand(server,command,out)
            else:
                # Print output
                data = self.sendcommand(server,command)
                if data and len(data) < 1024:
                    print data
                elif data:
                    print "Return data is over 1K, please use the -o option to redirect it to a file"
            if file:
                f.close()
        except TypeError:
            self.help()

if __name__ == "__main__":
    MyDHTClient().cmdlinestart()
