import unittest
import thread
from HashRing import Server
from MyDHTTable import MyDHTTable
from mydhtclient import MyDHTClient
__author__ = 'Johan'

class TestMyDHT(unittest.TestCase):

    def setUp(self):
        host = "localhost"
        port = 50140
        self.server = Server(host,port)
        self.dht = MyDHTClient()
        self.dht.verbose = False

    def test_1put(self):
        for i in range(10):
            response = self.dht.sendcommand(self.server,MyDHTTable.PUT,"key"+str(i),"value"+str(i))
            self.assertEquals(response,"PUT OK key"+str(i))

    def test_2get(self):
        for i in range(10):
            response = self.dht.sendcommand(self.server,MyDHTTable.GET,"key"+str(i))
            self.assertEquals(response,"value"+str(i))

    def test_3del(self):
        for i in range(10):
            response = self.dht.sendcommand(self.server,MyDHTTable.DEL,"key"+str(i))
            self.assertEquals(response,"DEL OK key"+str(i))

if __name__ == '__main__':
    unittest.main()