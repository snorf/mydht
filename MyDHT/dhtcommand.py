from time import time
import urllib

__author__ = 'Johan'
_block = 4096

class DHTCommand():
    PUT = 1
    GET = 2
    DEL = 3
    HASKEY = 4
    PURGE = 5
    LEAVE = 6
    REMOVE = 7
    JOIN = 8
    ADDNODE = 9
    WHEREIS = 10
    BALANCE = 11
    HTTPGET = 12
    HTTPGETKEY = 13
    UNKNOWN = 99
    allcommands = \
    {1: "PUT",
     2: "GET",
     3: "DEL",
     4: "HASKEY",
     5: "PURGE",
     6: "LEAVE",
     7: "REMOVE",
     8: "JOIN",
     9: "ADDNODE",
     10: "WHEREIS",
     11: "BALANCE",
     12: "HTTPGET",
     13: "HTTPGETKEY",
     99: "UNKNOWN"}
    SEPARATOR=chr(30) # This is the ASCII 30-character aka record delimiter

    def __init__(self,action=None,key=None,value=None,timestamp=None):
        """ Initialize a command with `key`, `action` and `value`
            if value is a file object read through it to get the size
            and then rewind it.
            `timestamp` is seconds since epoch and it will be set
            to current time if None.
        """
        if action is not None and action not in self.allcommands:
            raise Exception("Invalid command:",action)
        self.action = action or self.UNKNOWN
        self.key = str(key)
        self.value = value
        self.forwarded = False
        self.timestamp = timestamp or time()
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
            self.action = self.HTTPGET
        elif command.find(self.SEPARATOR) > 0:
            commands = command.split(self.SEPARATOR)
            self.size = int(commands[0])
            self.action = int(commands[1])
            self.key = commands[2]
            self.forwarded = (commands[3] == "True")
            self.timestamp = float(commands[4])
        elif command.startswith("GET /"):
            # Unquote the urlencoded key and remove the leading /
            self.action = self.HTTPGETKEY
            self.key = urllib.unquote(command.split(" ")[1])[1:]
        else:
            self.action = self.UNKNOWN
        return self

    def getmessage(self):
        """ Returns a padded message consisting of `size`:`command`:`value`:0...
        """
        message = str(self.size) + self.SEPARATOR + \
                  str(self.action) + self.SEPARATOR + \
                  (self.key or "") + self.SEPARATOR + \
                  str(self.forwarded) + self.SEPARATOR + \
                  str(self.timestamp) + self.SEPARATOR

        # Add padding up to _block
        message = message + ("0"*(_block-len(message)))
        return message
    
    def __str__(self):
        """ Return a reader friendly representation of the message
        """
        return self.allcommands.get(self.action) + "," + \
               (self.key or "") + "," + \
               str(self.size) + "," + \
               str(self.forwarded)