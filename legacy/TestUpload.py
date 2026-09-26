import filecmp
import glob
import unittest
from HashRing import Server
from dhtcommand import DHTCommand
from mydhtclient import MyDHTClient

__author__ = 'Johan'

class TestMyDHT(unittest.TestCase):

    def setUp(self):
        host = "localhost"
        self.ports = [50140] #range(50140,50144)
        self.servers = []
        for port in self.ports:
            self.servers.append(Server(host,port))
        self.dht = MyDHTClient(True)

    def testUploadFiles(self):
        """ Open files in upload/ in binary mode
            and send them to dht
        """
        for i,file in enumerate(glob.glob("upload/*")):
            with open(file, "rb") as f:
                command = DHTCommand(DHTCommand.PUT,file,f)
                response = self.dht.sendcommand(self.servers[i % len(self.ports)],command)
            self.assertEquals(response,"PUT OK " + file)
            command = DHTCommand(DHTCommand.PUT,"key for:"+file,"this is the key for:"+file)
            response = self.dht.sendcommand(self.servers[i % len(self.ports)],command)
            self.assertEquals(response,"PUT OK key for:" + file)

if __name__ == '__main__':
    unittest.main()
