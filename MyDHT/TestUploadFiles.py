import filecmp
import glob
import unittest
from HashRing import Server
from dhtcommand import DHTCommand
from mydhtclient import MyDHTClient

__author__ = 'Johan'

class TestMyDHT(unittest.TestCase):

    def setUp(self):
        self.server = Server("localhost",50140)
        self.dht = MyDHTClient()
        self.dht.verbose = True

    def test_1_UploadFiles(self):
        """ Open files in testfiles/ in binary mode
            and send them to dht
        """
        for file in glob.glob("testfiles/*"):
            if file.endswith(".downloaded"):
                continue
            with open(file, "rb") as f:
                command = DHTCommand(DHTCommand.PUT,file,f)
                response = self.dht.sendcommand(self.server,command)
            self.assertEquals(response,"PUT OK " + file)
            
    def test_2_DownloadFiles(self):
        """ Open files in testfiles/ in binary mode
            and send them to dht
        """
        for file in glob.glob("testfiles/*"):
            if file.endswith(".downloaded"):
                continue
            with open(file + ".downloaded", "wb") as f:
                command = DHTCommand(DHTCommand.GET,file)
                self.dht.sendcommand(self.server,command,f)
        
