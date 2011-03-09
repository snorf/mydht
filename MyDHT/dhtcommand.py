__author__ = 'Johan'
_block = 1024

class DHTCommand():
    PUT = 1
    GET = 2
    DEL = 3
    JOIN = 4
    ADDNODE = 5
    WHEREIS = 6
    COUNT = 7
    GETMAP = 8
    HTTPGET = 98
    UNKNOWN = 99
    allcommand = \
    {1: "PUT",
     2: "GET",
     3: "DEL",
     4: "JOIN",
     5: "ADDNODE",
     6: "WHEREIS",
     7: "COUNT",
     8: "GETMAP",
     98: "HTTPGET",
     99: "UNKNOWN"}
    SEPARATOR="|"

    def __init__(self,command=None,key=None,value=None):
        """ Initialize a command with `key`, `command` and `value`
            if value is a file object read through it to get the size
            and then rewind it.
        """
        if command is not None and command not in self.allcommand:
            raise Exception("Invalid command:",command)
        self.command = command or self.UNKNOWN
        self.key = str(key)
        self.value = value
        if isinstance(value,file):
            self.size = len(value.read())
            value.seek(0)
        else:
            self.size = len(value or [])

    def parse(self,command):
        """ Parse a padded message on the server side
        Takes care of regular HTTP GET (for debugging purposes)
        """
        if command[0:14] == "GET / HTTP/1.1":
            self.command = self.HTTPGET
        elif command.find(self.SEPARATOR) > 0:
            commands = command.split(self.SEPARATOR)
            self.size = int(commands[0])
            self.command = int(commands[1])
            if len(commands) == 4:
                self.key = commands[2]
        else:
            self.command = self.UNKNOWN
        return self

    def getmessage(self):
        """ Returns a padded message consisting of `size`:`command`:`value`:0...
        """
        message = str(self.size) + self.SEPARATOR + \
                  str(self.command) + self.SEPARATOR + \
                  (self.key or "") + self.SEPARATOR
        # Add padding up to _block
        message = message + ("0"*(_block-len(message)))
        return message
    
    def __str__(self):
        """ Return a reader friendly representation of the message
        """
        return self.allcommand.get(self.command) + "," + (self.key or "") + ","+str(self.size)