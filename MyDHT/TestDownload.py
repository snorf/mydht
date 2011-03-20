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

    def testDownloadFiles(self):
        """ For every file in upload/ download them
            from DHT to download/
        """
        for i,file in enumerate(glob.glob("upload/*")):
            with open(file.replace("upload","download"), "wb") as f:
                command = DHTCommand(DHTCommand.GET,file)
                self.dht.sendcommand(self.servers[i % len(self.ports)],command,f)
            command = DHTCommand(DHTCommand.GET,"key for:"+file)
            print self.dht.sendcommand(self.servers[i % len(self.ports)],command)

if __name__ == '__main__':
    unittest.main()
