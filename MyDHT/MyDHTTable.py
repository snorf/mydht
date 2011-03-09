from cStringIO import StringIO
from HashRing import HashRing
from dhtcommand import DHTCommand

__author__ = 'Johan'

class MyDHTTable():
    def __init__(self):
        self.map = {}

    def count(self):
        """ Returns number of keys in map
        """
        return len(self.map)

    def __str__(self):
        values = []
        for key in self.map.keys():
            values.append(key + ": " + self.map[key])
        return "\n".join(values)

    def getsizewithsuffix(self,size):
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
        webpage.write("key count: "+str(len(self.map)))
        webpage.write("<table border=\"1\">\n<tr>\n<td>key</td>\n<td>size</td>\n<td>hash</td>\n</tr>\n")
        size = 0
        for key in self.map.keys():
            webpage.write("<tr>\n")
            webpage.write("<td>" + key + "</td>")
            size += len(self.map[key])
            webpage.write("<td>" + self.getsizewithsuffix(len(self.map[key])) + "</td>")
            webpage.write("<td>" + str(HashRing().gen_key(key)) + "</td>")
            webpage.write("</tr>\n")
        webpage.write("<br/>Total size: " + self.getsizewithsuffix(size))

        webpage.write("</body>\n</html>\n")
        return webpage.getvalue()

    def perform(self,cmd):
        """ Perform `cmd` on this map
            return BAD_COMMAND if the command is invalid
        """
        if cmd.command == DHTCommand.PUT:
            self.map[cmd.key] = cmd.value
            return "PUT OK "+cmd.key
        elif cmd.command == DHTCommand.GET:
            try:
                return self.map[cmd.key]
            except KeyError:
                return "ERR_VALUE_NOT_FOUND"
        elif cmd.command == DHTCommand.DEL:
            try:
                del self.map[cmd.key]
                return "DEL OK "+cmd.key
            except KeyError:
                return "ERR_VALUE_NOT_FOUND"
        elif cmd.command == DHTCommand.COUNT:
            return "count: " + str(self.map.count())
        elif cmd.command == DHTCommand.GETMAP:
            return "map:\n" + str(self.map)
        elif cmd.command == DHTCommand.HTTPGET:
            return self.gethtml()
        else:
            self.debug("Invalid command",str(cmd))
            return "BAD_COMMAND: "+str(cmd)