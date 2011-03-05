import os
import signal
import time
import unittest
from HashRing import Server
from mydht import MyDHT

__author__ = 'Johan'
host = "localhost"
port = 50148
PUT = "put"
GET = "get"
DEL = "del"
NODES=1
class TestLargeNetwork(unittest.TestCase):
#    def startservernode(self,host,port,othernode=None):
#        """ Starts a DHT
#        """

    def setUp(self):
        print "setting up"
        self.server = Server(host,port)
        self.dht = MyDHT()
        # Starts a new server
        nodes = 0
        verbose = True
        joinserver = None
        self.pids = []
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
                self.pids.append(pid)
                print "Forked a new DHT server at",str(port+nodes)
            nodes += 1

    def tearDown(self):
        for pid in self.pids:
            print "Waiting for ",pid
            os.kill(pid,signal.SIGINT)
            os.waitpid(pid,0)

    def test_1put(self):
        time.sleep(5)
        for i in range(100):
            server, response = self.dht.sendcommand(self.server,PUT,"key"+str(i),"value"+str(i))
            self.assertEquals(response,"OK")
        for i in range(100):
            server, response = self.dht.sendcommand(self.server,GET,"key"+str(i))
            self.assertEquals(response,"value"+str(i))
        for i in range(100):
            server, response = self.dht.sendcommand(self.server,DEL,"key"+str(i))
            self.assertEquals(response,"OK")

if __name__ == '__main__':
    unittest.main()
