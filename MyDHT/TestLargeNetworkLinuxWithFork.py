import os
import signal
import time
import unittest
from HashRing import Server
from MyDHTTable import MyDHTTable
from mydhtclient import MyDHTClient
from mydhtserver import MyDHT

__author__ = 'Johan'
host = "localhost"
port = 50400
NODES=3
class TestLargeNetwork(unittest.TestCase):
#    def startservernode(self,host,port,othernode=None):
#        """ Starts a DHT
#        """

    def setUp(self):
        print "setting up"
        self.server = Server(host,port)
        self.dht = MyDHTClient()
        self.dht.verbose = True
        # Starts a new server
        nodes = 0
        verbose = True
        joinserver = None
        self.pids = []
        self.servers = []
        while nodes < NODES:
            print "Running"
            if nodes > 0:
                joinserver=host+":"+str(port)
            logfile = host+"_"+str(port+nodes)+".txt"
            print "logfile:",logfile
            pid = os.fork()
            if pid == 0:
                MyDHT().start(host,port+nodes,joinserver,verbose,logfile,is_process=True)
            else:
                self.servers.append(Server(host,port+nodes))
                self.pids.append(pid)
                print "Forked a new DHT server at",str(port+nodes)
            time.sleep(1)
            nodes += 1

    def tearDown(self):
        for pid in self.pids:
            print "Waiting for ",pid
            os.kill(pid,signal.SIGINT)
            os.waitpid(pid,0)

    def test_1put(self):
        time.sleep(2)
        for i in range(100):
            response = self.dht.sendcommand(self.servers[i % NODES],MyDHTTable.PUT,"key"+str(i),"value"+str(i))
            self.assertEquals(response,"PUT OK key"+str(i))
        for i in range(100):
            response = self.dht.sendcommand(self.servers[i % NODES],MyDHTTable.GET,"key"+str(i))
            self.assertEquals(response,"value"+str(i))
        for i in range(100):
            response = self.dht.sendcommand(self.servers[i % NODES],MyDHTTable.DEL,"key"+str(i))
            self.assertEquals(response,"DEL OK key"+str(i))

if __name__ == '__main__':
    unittest.main()
