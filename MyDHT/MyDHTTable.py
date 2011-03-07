from cStringIO import StringIO
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

    def gethtml(self,hostname):
        webpage = StringIO()
        webpage.write("<html>\n<head>Status for ")
        webpage.write(hostname)
        webpage.write("<br />\n</head>\n<body>\n")
        for key in self.map.keys():
            webpage.write(key + ": " + self.map[key] + "<br />")
        webpage.write("</body>\n</html>\n")
        return webpage.getvalue()

    def perform(self,cmd):
        """ Perform `command` with `key` and `value`
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
        else:
            return "BAD_COMMAND: "+str(cmd)