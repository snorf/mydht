from cStringIO import StringIO
from HashRing import HashRing
from dhtcommand import DHTCommand

__author__ = 'Johan'

class MyDHTTable():
    """ Represents the hash table
        This is really just a dictionary with some convenience methods.
        Most of it is used to render a html-page for debugging purposes.
    """
    def __init__(self,hash_ring=None):
        self._map = {}

    def __str__(self):
        """ Returns a string representation of the map
        """
        values = []
        for key in self._map.keys():
            values.append(key + ": " + self._map[key])
        return "\n".join(values)

    def set_hash_ring(self,hash_ring):
        self.hash_ring = hash_ring

    def get_keys(self):
        return self._map.iterkeys()

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
            columns for key, size and hash.
        """
        webpage = StringIO()
        webpage.write("<html>\n<head>DHT status page<br />\n")
        webpage.write("</head>\n<body>\n")
        webpage.write("key count: " + str(len(self._map)) + "</br>")
        webpage.write("Ring is: ")
        for server in self.hash_ring.get_nodelist():
            webpage.write("<a href=http://"+str(server) + ">" + str(server) + "</a> ")
        webpage.write("</br>")

        webpage.write("<table border=\"1\">\n<tr>\n<td>key</td>\n<td>size</td>\n<td>hash</td>\n<td>replicas</td></tr>\n")
        size = 0
        for key in self._map.keys():
            webpage.write("<tr>\n")
            webpage.write("<td>" + key + "</td>")
            size += len(self._map[key])
            webpage.write("<td>" + self.getsizewithsuffix(len(self._map[key])) + "</td>")
            webpage.write("<td>" + str(HashRing().gen_key(key)) + "</td>")
            webpage.write("<td>")
            for server in self.hash_ring.get_replicas(key):
                webpage.write("<a href=http://"+str(server) + ">" + str(server) + "</a> ")
            webpage.write("</td>")
            webpage.write("</tr>\n")
        webpage.write("</table>")
        webpage.write("<br/>Total size: " + self.getsizewithsuffix(size))
        webpage.write("</body>\n</html>\n")
        return webpage.getvalue()

    def perform(self,cmd):
        """ Perform `cmd` on this map
            return BAD_COMMAND if the command is invalid
        """
        if cmd.command == DHTCommand.PUT:
            self._map[cmd.key] = cmd.value
            return "PUT OK "+cmd.key
        elif cmd.command == DHTCommand.GET:
            try:
                return self._map[cmd.key]
            except KeyError:
                return "ERR_VALUE_NOT_FOUND"
        elif cmd.command == DHTCommand.DEL:
            try:
                del self._map[cmd.key]
                return "DEL OK "+cmd.key
            except KeyError:
                return "ERR_VALUE_NOT_FOUND"
        elif cmd.command == DHTCommand.HASKEY:
            return str(self._map.has_key(cmd.key))
        elif cmd.command == DHTCommand.COUNT:
            return "count: " + str(self._map.count())
        elif cmd.command == DHTCommand.GETMAP:
            return "map:\n" + str(self._map)
        elif cmd.command == DHTCommand.HTTPGET:
            return self.gethtml()
        else:
            return "BAD_COMMAND: "+str(cmd)