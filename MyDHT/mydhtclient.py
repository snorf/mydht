from _socket import *
import collections
from cmdapp import CmdApp

__author__ = 'Johan'
_block = 1024

class MyDHTClient(CmdApp):
    def sendcommand(self,server,command,*values):
        """ Sends a `command` to a `server` in the ring
        """
        # Check if server is a list of servers
        if isinstance(server,collections.Iterable):
            fromserver, data = None, None
            for srv in server:
                try:
                    data = self.sendcommand(srv,command,*values)
                    if command == "get":
                        break
                except:
                    # Server is down?
                    continue
            return data

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
            return "".join(data)
        except Exception, e:
            print "Error connecting to server:",e.message
            return None
