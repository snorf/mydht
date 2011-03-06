from _socket import *
import collections
import traceback
from MyDHTTable import MyDHTTable
from cmdapp import CmdApp
from cStringIO import StringIO

__author__ = 'Johan'
_block = 1024

class MyDHTClient(CmdApp):
    def sendcommand(self,server,command,key=None,value=None):
        """ Sends a `command` to a `server` in the ring
        """
        for retry in range(3):
            self.debug("sending command to:", str(server), command, str(key), str(value),"try number",retry)
            sock = socket(AF_INET, SOCK_STREAM)

            try:
                sock.connect((server.bindaddress()))
                sock.send(command + "\n")
                sock.send(str(key or "") + "\n")

                # Send value to another server
                if value:
                    sock.send(value + "\n")

                # Using pseudofile
                data = StringIO()
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
                if command in [MyDHTTable.PUT,MyDHTTable.DEL]:
                    if data.getvalue().startswith(command+" OK"):
                        return data.getvalue()
                print "Error connecting to server:"
                print '-'*60
                traceback.print_exc()
                print '-'*60
        return None
