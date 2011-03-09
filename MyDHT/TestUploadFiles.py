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
            with open(file, "rb") as f:
                response = self.dht.sendcommand(self.server,DHTCommand.PUT,file,f)
            self.assertEquals(response,"PUT OK " + file)
            
    def test_2_DownloadFiles(self):
        """ Open files in testfiles/ in binary mode
            and send them to dht
        """
        for file in glob.glob("testfiles/*"):
            with open(file + ".downloaded", "wb") as f:
                response = self.dht.sendcommand(self.server,DHTCommand.GET,file)
                f.write(response)
        
