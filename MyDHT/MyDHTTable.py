__author__ = 'Johan'

class MyDHTTable():
    PUT = "PUT"
    GET = "GET"
    DEL = "DEL"
    
    def __init__(self):
        self.map = {}

    def count(self):
        """ Returns number of keys in map
        """
        return len(self.map)

    def __str__(self):
         return "\n".join(map(lambda keyvalue: str(keyvalue[0]) + ":" + str(keyvalue[1]), self.map.viewitems()))

    def perform(self,command,key,value):
        """ Perform `command` with `key` and `value`
        """
        if command == "PUT":
            self.map[key] = value
            return "PUT OK "+key
        elif command == "GET":
            try:
                return self.map[key]
            except KeyError:
                return "ERR_VALUE_NOT_FOUND"
        elif command == "DEL":
            try:
                del self.map[key]
                return "DEL OK "+key
            except KeyError:
                return "ERR_VALUE_NOT_FOUND"
        else:
            return "BAD_COMMAND: "+command