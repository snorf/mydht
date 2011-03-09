from _socket import *
import traceback
from HashRing import Server
from cmdapp import CmdApp
from StringIO import StringIO
from dhtcommand import DHTCommand

__author__ = 'Johan'
_block = 1024

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
        """


    def sendcommand(self,server,command,key=None,value=None,outfile=None):
        """ Sends a `command` to a `server` in the ring
            `outfile` is used when the client wants the output
            value written to an output stream.
        """

        # If command isn't already a DHTCommand, create one
        if not isinstance(command,DHTCommand):
            message = DHTCommand(command,key,value)
        else:
            message = command
            
        for retry in range(3):
            self.debug("sending command to:", str(server), str(message),"try number",retry)
            sock = socket(AF_INET, SOCK_STREAM)

            try:
                sock.connect((server.bindaddress()))
                # If value send the command and the size of value
                sock.send(message.getmessage())

                # Send value to another server
                if message.value:
                    if not isinstance(message.value,file):
                        # Copy string contents to StringIO object
                        value = StringIO(message.value)
                        
                    totalsent = 0
                    while totalsent < message.size:
                        sent = sock.send(value.read(_block))
                        if sent == 0:
                            raise RuntimeError("socket connection broken")
                        totalsent += sent

                # Using pseudofile
                data = StringIO()
                while 1:
                    incoming = sock.recv(_block)
                    if not incoming: break
                    if outfile:
                        outfile.write(incoming)
                    else:
                        data.write(incoming)

                sock.close()
                return data.getvalue()
            except:
                # If it was a PUT or DEL we can return safely
                # if we have already gotten an OK from the server
                #if command.command in [MyDHTTable.PUT,MyDHTTable.DEL]:
                #    if data.getvalue().startswith(command+" OK"):
                #        return data.getvalue()
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
            key = self.getarg("-k") or self.getarg("--key")
            outfile = self.getarg("-o") or self.getarg("--outfile")
            server = Server(host,port)
            command = self.getarg('-c') or self.getarg('--command')
            value = self.getarg("-val") or self.getarg("--value")
            self.debug("command:",\
            str(server),command,key,value)
            if command is None or key is None or server is None:
                self.help()
            self.sendcommand(server,command,key,value,outfile)
        except TypeError:
            self.help()
        self.client()

if __name__ == "__main__":
    MyDHTClient().cmdlinestart()