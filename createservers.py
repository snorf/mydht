from HashRing import Server
from cmdapp import CmdApp

__author__ = 'Johan'

class CreateServers(CmdApp):
    def __init__(self):
        """ A helpful script for starting multiple servers from command line """
        CmdApp.__init__(self)
        self.usage = \
        """
           -h, --hostname
             specify hostname (default: localhost)
           -p, --port
             specify first port (default: 50140)
           -n, --no_servers
             specify number of servers
           -s, --server
             join existing server
        """

    def get_start_string(self,host,port,remoteserver=None):
        start_string = "python mydhtserver.py -h " + host + " -p " + str(port)
        if remoteserver:
            start_string += " -s " + remoteserver
        start_string += " -v -l " + host + "-" + str(port) + ".log &"
        return start_string

    def cmdlinestart(self):
        """ Parse command line parameters and start client
        """
        try:
            if self.getopt("--help"):
                self.help()
            port = int( self.getarg("-p") or self.getarg("--port",50140))
            host = self.getarg("-h") or self.getarg("--hostname","localhost")
            server = Server(host,port)
            no_serves = self.getarg("-n") or self.getarg("--no_servers", 5)
            no_serves = int(no_serves)
            remoteserver = self.getarg("-s") or self.getarg("-server")
            if not remoteserver:
                remoteserver = str(server)
                print self.get_start_string(host,port)
            else:
                print self.get_start_string(host,port,remoteserver)

            for i in range(1,no_serves):
                print self.get_start_string(host,port+i,remoteserver)

        except TypeError:
            self.help()

if __name__ == "__main__":
    CreateServers().cmdlinestart()