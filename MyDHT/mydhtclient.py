from _socket import *
import collections
import string
import traceback
from HashRing import Server
from MyDHTTable import MyDHTTable
from cmdapp import CmdApp
from cStringIO import StringIO
from dhtcommand import DHTCommand, DHTCommand

__author__ = 'Johan'
_block = 1024

class MyDHTClient(CmdApp):
    def __init__(self):
        """A MyDHT client for interacting with MyDHT servers
        """
        CmdApp.__init__(self)
        self.usage = \
        """
           -c, --command
             put, get, del
           -h, --hostname
             specify hostname (default: localhost)
        """


    def sendcommand(self,server,command,key=None,value=None):
        """ Sends a `command` to a `server` in the ring
        """

        # If command isn't already a DHTCommand, create one
        if not isinstance(command,DHTCommand):
            message = DHTCommand(command,key,value)
        else:
            message = command
            
        for retry in range(3):
            self.debug("sending command to:", str(server), str(message),"try number",retry)
            sock = socket(AF_INET, SOCK_STREAM)
            data = StringIO()

            try:
                sock.connect((server.bindaddress()))
                # If value send the command and the size of value
                sock.send(message.getmessage())

                # Send value to another server
                if message.value:
                    totalsent = 0
                    while totalsent < message.size:
                        sent = sock.send(message.value[totalsent:])
                        if sent == 0:
                            raise RuntimeError("socket connection broken")
                        totalsent += sent

                # Using pseudofile
                while 1:
                    incoming = sock.recv(_block)
                    if not incoming: break
                    data.write(incoming)

                sock.close()
                #self.debug("closed connection to server")
                #self.debug("response from server",data.getvalue())
                # Return the concatenated data
                return data.getvalue()
            except:
                # If it was a PUT or DEL we can return safely
                # if we have already gotten an OK from the server
                if command.command in [MyDHTTable.PUT,MyDHTTable.DEL]:
                    if data.getvalue().startswith(command+" OK"):
                        return data.getvalue()
                print "Error connecting to server:"
                print '-'*60
                traceback.print_exc()
                print '-'*60
        return None

    def cmdlinestart(self):
        """ Parse command line parameters and start client
        """
        try:
            port = int( self.getarg("-p") or self.getarg("--port",50140))
            host = self.getarg("-h") or self.getarg("--hostname","localhost")
            key = self.getarg("-k") or self.getarg("-key")
            server = Server(host,port)
            command = self.getarg('-c') or self.getarg('-command')
            value = self.getarg("-val") or self.getarg("-value")
            self.debug("command:",\
            str(self.server),self.command,self.key,self.value)
            if self.command is None or self.key is None or self.server is None:
                self.help()
            self.sendcommand(self.server,self.command,self.key,self.value)
        except TypeError:
            self.help()
        self.client()

if __name__ == "__main__":
    MyDHTClient().cmdlinestart()