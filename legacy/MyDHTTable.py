from cStringIO import StringIO
import threading
import urllib
from HashRing import HashRing
from dhtcommand import DHTCommand

__author__ = 'Johan'

class MyDHTTable():
    """ Represents the hash table
        This is really just a dictionary with some convenience methods.
        Most of it is used to render a html-page for debugging purposes.
    """
    def __init__(self,server_name,hash_ring):
        self._map = {}
        self._timemap = {}
        self.hash_ring = hash_ring
        self.server_name = server_name
        self._lock = threading.RLock()

    def __str__(self):
        """ Returns a string representation of the map
        """
        values = []
        for key in self._map.keys():
            values.append(key + ": " + self._map[key])
        return "\n".join(values)

    def get_keys(self):
        """ Returns all keys currently in the map
            Lock first, just in case
        """
        self._lock.acquire()
        keys = self._map.keys()
        self._lock.release()
        return keys
    
    def getsizewithsuffix(self,size):
        """ Adds a suffix to `size` and returns
            "`size` suffix"
        """
        if size > 1024*1024*1024:
            return str(size/(1024*1024*1024)) + " GB"
        elif size > 1024*1024:
            return str(size/(1024*1024)) + " MB"
        elif size > 1024:
            return str(size/1024) + " KB"
        else:
            return str(size) + " B"

    def gethtml(self):
        """ Generates a html representation of the map with
            columns for key, size, time, hash and replicas.
        """
        webpage = StringIO()
        webpage.write("<html>\n<head><title>DHT status page for " + str(self.server_name) + "</title>\n")
        webpage.write("</head>\n<body>\nDHT status page for " + str(self.server_name) + "<br />")
        webpage.write("key count: " + str(len(self._map)) + "</br>")
        webpage.write("Ring is:<br />")
        for server in self.hash_ring.get_nodelist():
            webpage.write("<a href=http://"+str(server) + ">" + str(server) + "</a><br />")
        webpage.write("</br>")
        webpage.write("Number of replicas: " + str(self.hash_ring.replicas) + "<br /><br />")

        webpage.write("<table border=\"1\">\n<tr>\n<td>key</td>\n<td>size</td>\n<td>time</td>\n<td>hash</td>\n<td>replicas</td></tr>\n")
        size = 0
        for key in self._map.keys():
            webpage.write("<tr>\n")
            webpage.write("<td><a href=/"+ urllib.quote(key) + ">" + key + "</a></td>")
            size += len(self._map[key])
            webpage.write("<td>" + self.getsizewithsuffix(len(self._map[key])) + "</td>")
            webpage.write("<td>" + str(self._timemap.get(key)) + "</td>")
            webpage.write("<td>" + str(HashRing().gen_key(key)) + "</td>")
            webpage.write("<td>")
            for server in self.hash_ring.get_replicas(key):
                webpage.write("<a href=http://"+str(server) + "/" + urllib.quote(key) + ">" + str(server) + "</a> ")
            webpage.write("</td>")
            webpage.write("</tr>\n")
        webpage.write("</table>")
        webpage.write("<br/>Total size: " + self.getsizewithsuffix(size))
        webpage.write("</body>\n</html>\n")
        return webpage.getvalue()

    def perform(self,command):
        """ Perform `command` on this map
            return BAD_COMMAND if the command is invalid
        """
        self._lock.acquire()
        
        if command.action == DHTCommand.PUT:
            """ Put key and value in map """
            self._map[command.key] = command.value
            self._timemap[command.key] = command.timestamp
            status = "PUT OK "+command.key

        elif command.action == DHTCommand.GET or command.action == DHTCommand.HTTPGETKEY:
            """ Get value from map if key exists """
            if command.key in self._map:
                status = self._map[command.key]
            else:
                status = "ERR_VALUE_NOT_FOUND"

        elif command.action == DHTCommand.DEL:
            """ Delete key from map if it exists """
            if command.key in self._map:
                del self._map[command.key]
                status = "DEL OK "+command.key
            else:
                status = "ERR_VALUE_NOT_FOUND"

        elif command.action == DHTCommand.HASKEY:
            """ Return the timestamp if key is found, else 0.0 (epoch) """
            if command.key in self._timemap and command.key in self._map:
                status = str(self._timemap.get(command.key))
            else: status = "0.0"

        elif command.action == DHTCommand.HTTPGET:
            """ Return the status web page """
            status = self.gethtml()

        elif command.action == DHTCommand.PURGE:
            """ Remove all keys in this map that don't belong here """
            for key in self._map.keys():
                if self.server_name not in self.hash_ring.get_replicas(key):
                    del self._map[key]
            status = "PURGE ok"
        else:
            status = "BAD_COMMAND: "+str(command)

        self._lock.release()
        return status