import os
import time
import unittest
from HashRing import Server
from mydhtserver import MyDHT

__author__ = 'Johan'
host = "localhost"
port = 50141
PUT = "put"
GET = "get"
DEL = "del"
NODES=5
class TestLargeNetwork(unittest.TestCase):
#    def startservernode(self,host,port,othernode=None):
#        """ Starts a DHT
#        """

    @classmethod
    def setUpClass(cls):
        cls._dht = MyDHT()
        cls._dht.verbose = False
        # Starts a new server
        nodes = 0
        verbose = True
        joinserver = None
        while nodes < NODES:
            print "Running"
            if nodes > 0:
                joinserver=host+":"+str(port)
            logfile = host+"_"+str(port+nodes)+".txt"
            pid = os.fork()
            if pid == 0:
                print "From child"
                MyDHT().start(host,port+nodes,joinserver,verbose,nodes)
            else:
                print "Forked a new DHT server at",str(port+nodes)
            nodes += 1

    def setUp(self):
        self.server = Server(host,port)
        self.dht = MyDHT()

    @classmethod
    def tearDownClass(cls):
        print "tearing down class"
        #cls._servers[host+str(port)].stop()

    def test_1put(self):
        time.sleep(5)
        for i in range(100):
            server, response = self.dht.sendcommand(self.server,PUT,"key"+str(i),"value"+str(i))
            self.assertEquals(response,"OK")

    def test_5del(self):
        for i in range(100):
            server, response = self.dht.sendcommand(self.server,DEL,"key"+str(i))
            self.assertEquals(response,"OK")

    def test_2get(self):
        for i in range(100):
            server, response = self.dht.sendcommand(self.server,GET,"key"+str(i))
            self.assertEquals(response,"value"+str(i))

#    def test_4wait(self):
#        while 1:
#            True


if __name__ == '__main__':
    unittest.main()