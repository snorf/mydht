import unittest
import thread
from HashRing import Server
from MyDHTTable import MyDHTTable
from mydhtclient import MyDHTClient
__author__ = 'Johan'
verylongstring = "i"*10
number_of_values=10
class TestMyDHT(unittest.TestCase):

    def setUp(self):
        host = "localhost"
        self.ports = [50140,50141,50142]
        self.servers = []
        for port in self.ports:
            self.servers.append(Server(host,port))
        self.dht = MyDHTClient()
        self.dht.verbose = True

    def test_1put(self):
        for i in range(number_of_values):
            response = self.dht.sendcommand(self.servers[i % len(self.ports)],MyDHTTable.PUT,"key"+str(i),"value"+str(i)+verylongstring)
            self.assertEquals(response,"PUT OK key"+str(i))

    def test_2where_is(self):
        for i in range(number_of_values):
            response = self.dht.sendcommand(self.servers[i % len(self.ports)],"whereis","key"+str(i))
            print response
        for server in self.servers:
            response = self.dht.sendcommand(server,"count")
            print response
        for server in self.servers:
            response = self.dht.sendcommand(server,"getmap")
            print response

    def test_3get(self):
        for i in range(number_of_values):
            response = self.dht.sendcommand(self.servers[i % len(self.ports)],MyDHTTable.GET,"key"+str(i))
            print len(response)
            self.assertEquals(response,"value"+str(i)+verylongstring)

    def test_4del(self):
        for i in range(number_of_values):
            response = self.dht.sendcommand(self.servers[i % len(self.ports)],MyDHTTable.DEL,"key"+str(i))
            self.assertEquals(response,"DEL OK key"+str(i))

if __name__ == '__main__':
    unittest.main()