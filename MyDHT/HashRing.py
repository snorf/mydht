import hashlib

class Server():
    """ Conveience class that holds a host and a port
        and provides a method for getting them as a tuple
        for binding.
    """
    def __init__(self,host,port):
        self.host = host
        self.port = int(port)

    def __str__(self):
        """ Returns `host`:`port`
        """
        return self.host + ":" + str(self.port)

    def bindaddress(self):
        """ Returns (`host`,`port`)
        """
        return self.host,self.port

class HashRing(object):
    def __init__(self, nodes=None, distribution_points=3, replicas=3):
        """Manages a hash ring.

        `nodes` is a list of objects that have a proper __str__ representation.
        `distribution_points` indicates how many virtual points should be used pr. node,
        distribution_points are required to improve the distribution.
        """
        self.distribution_points = distribution_points
        self.replicas = replicas

        self.ring = dict()
        self._sorted_keys = []

        if nodes:
            for node in nodes:
                self.add_node(node)

    def __str__(self):
        return " ".join(map(lambda server: str(server),self.ring.values()))

    def add_node(self, node):
        """Adds a `node` to the hash ring (including a number of replicas).
           If node is not already a `Server` it will become one.
        """
        if not isinstance(node,Server):
            host, port = node.split(":")
            node = Server(host,port)

        for i in xrange(0, self.distribution_points):
            key = self.gen_key('%s:%s' % (node, i))
            self.ring[key] = node
            if key not in self._sorted_keys:
                self._sorted_keys.append(key)

        self._sorted_keys.sort()

    def remove_node(self, node):
        """Removes `node` from the hash ring and its replicas.
        """
        for i in xrange(0, self.distribution_points):
            key = self.gen_key('%s:%s' % (node, i))
            if self.ring.has_key(key):
                del self.ring[key]
            if key in self._sorted_keys:
                self._sorted_keys.remove(key)


    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        return self.get_node_pos(string_key)[0]

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.ring:
            return None, None

        key = self.gen_key(string_key)

        nodes = self._sorted_keys
        for i in xrange(0, len(nodes)):
            node = nodes[i]
            if key <= node:
                return self.ring[node], i

        return self.ring[nodes[0]], 0

    def get_replicas(self, string_key, exclude_server=None):
        """ Given a `string_key` return the replica nodes that can hold the key
            `exclude_server` will be removed from the list before return.
            The replica nodes is just 3 consecutive nodes at the moment
        """

        if not self.ring:
            return None

        nodelist = []
        for key in self.get_nodes(string_key):
            if key not in nodelist:
                nodelist.append(key)
            if len(nodelist) == self.replicas:
                break

        if exclude_server in nodelist:
            nodelist.remove(exclude_server)

        return nodelist

    def get_nodes(self, string_key):
        """Given a string key it returns the nodes as a generator that can hold the key.

        The generator is never ending and iterates through the ring
        starting at the correct position.
        """
        if not self.ring:
            yield None, None

        node, pos = self.get_node_pos(string_key)
        for key in self._sorted_keys[pos:]:
            yield self.ring[key]

        while True:
            for key in self._sorted_keys:
                yield self.ring[key]

    def get_nodelist(self):
        """ Given a string key returns the nodes as a set.
        """
        if not self.ring:
            return None

        nodelist = []
        for key in self._sorted_keys:
            if self.ring[key] not in nodelist:
                nodelist.append(self.ring[key])

        return nodelist


    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.
        """
        m = hashlib.sha1()
        m.update(key)
        return long(m.hexdigest(), 16)