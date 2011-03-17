from _socket import *
import sys
import traceback
from HashRing import Server
from cmdapp import CmdApp
from StringIO import StringIO
from dhtcommand import DHTCommand

__author__ = 'Johan'
_block = 4096

class MyDHTClient(CmdApp):
    def __init__(self):
        """A MyDHT client for interacting with MyDHT servers
        """
        CmdApp.__init__(self)
        self.usage = \
        """
           -h, --hostname
             specify hostname (default: localhost)
           -p, --port
             specify port (default: 50140)
           -c, --command
             put, get, del
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
        """
        received = 0
        data = StringIO()
        while received < size:
            incoming = socket.recv(_block)
            if not incoming: break
            received += len(incoming)
            if isinstance(outstream,file):
                # If outstream is a file, write to it
                outstream.write(incoming)
            else:
                data.write(incoming)
        return data.getvalue()

    def send_length_to_socket(self,length,socket):
        """ Create a new length packet and send it to `socket`
        """
        length = str(length) + "|"
        length = length + ("0"*(_block-len(length)))
        socket.send(length)

    def read_length_from_socket(self,socket):
        """ Read the length packet from a socket.
            The packet is length + | + zero padding.
            return it as an int.
        """
        length = socket.recv(_block)
        length = int(length.split("|")[0])
        return length

    def sendcommand(self,server,command,outstream=None):
        """ Sends a `command` to a `server` in the ring
            `outstream` is used when the client wants the output
            value written to an output stream.
        """

        # If command isn't already a DHTCommand, create one

        for retry in range(3):
            self.debug("sending command to:", str(server), str(command),"try number",retry)
            sock = socket(AF_INET, SOCK_STREAM)

            try:
                sock.connect((server.bindaddress()))
                # If value send the command and the size of value
                sock.send(command.getmessage())

                # Send value to another server
                if command.value:
                    self.send_to_socket(command.value,command.size,sock)

                length = self.read_length_from_socket(sock)
                self.debug("receiving a file with size ",length)
                data = self.read_from_socket(length,sock,outstream)

                sock.close()
                if len(data) < 1024:
                    return data
                else:
                    return "Return data is over 1K, please use the -f option to redirect it to a file"
            except:
                print "Error connecting to server:"
                print '-'*60
                traceback.print_exc()
                print '-'*60
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

            self.debug("command:",\
            str(server),command,key,value)
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
                print self.sendcommand(server,command)

            if file:
                f.close()
        except TypeError:
            self.help()

if __name__ == "__main__":
    MyDHTClient().cmdlinestart()