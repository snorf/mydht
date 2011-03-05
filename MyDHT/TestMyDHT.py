import unittest
import thread
from HashRing import Server
from mydht import MyDHT

__author__ = 'Johan'

PUT = "put"
GET = "get"
DEL = "del"

class TestMyDHT(unittest.TestCase):

    def setUp(self):
        host = "localhost"
        port = 50141
        self.server = Server(host,port)
        self.dht = MyDHT()
        self.dht.verbose = False

    def test_1put(self):
        for i in range(10):
            server, response = self.dht.sendcommand(self.server,PUT,"key"+str(i),"value"+str(i))
            self.assertEquals(response,"OK")

    def test_3del(self):
        for i in range(10):
            server, response = self.dht.sendcommand(self.server,DEL,"key"+str(i))
            self.assertEquals(response,"OK")

    def test_2get(self):
        for i in range(10):
            server, response = self.dht.sendcommand(self.server,GET,"key"+str(i))
            self.assertEquals(response,"value"+str(i))

if __name__ == '__main__':
    unittest.main()