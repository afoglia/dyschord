import xmlrpclib
import socket
import logging
import json

# Timeout XML-RPC ServerProxy code.
#
# Taken from
# <http://stackoverflow.com/questions/372365/set-timeout-for-xmlrpclib-serverproxy> with modification to get it to run in Python 2.7.

import httplib

class TimeoutHTTPConnection(httplib.HTTPConnection):
  def connect(self):
    httplib.HTTPConnection.connect(self)
    self.sock.settimeout(self.timeout)

class TimeoutHTTP(httplib.HTTP):
  _connection_class = TimeoutHTTPConnection
  def set_timeout(self, timeout):
    self._conn.timeout = timeout

class TimeoutTransport(xmlrpclib.Transport):
  def __init__(self, timeout=10, *l, **kw):
    xmlrpclib.Transport.__init__(self,*l,**kw)
    self.timeout=timeout
  def make_connection(self, host):
    conn = TimeoutHTTPConnection(host)
    conn.timeout = self.timeout
    return conn

class TimeoutServerProxy(xmlrpclib.ServerProxy):
  def __init__(self,uri,timeout=10,*l,**kw):
    kw['transport']=TimeoutTransport(
      timeout=timeout, use_datetime=kw.get('use_datetime',0))
    xmlrpclib.ServerProxy.__init__(self,uri,*l,**kw)


# Node description functions
#
# Instead of sending the full objects, when we need to send node info
# (i.e. for pointing to the next item in the chain), we'll pass a
# description instead
class ProxyTranslation(object) :
  def __init__(self, url='') :
    # Default url to use
    self.url = url
    self.local_nodes = {}

  def to_descr(self, node) :
    return {"id": node.id, "url": getattr(node, "url", self.url)}

  def from_descr(self, descr) :
    try :
      node_id = descr["id"]
    except KeyError :
      proxy = NodeProxy(url=descr["url"], id=descr.get("id"))
      node_id = proxy.id
    else :
      proxy = None
    try :
      return self.local_nodes[node_id]
    except KeyError :
      if proxy is not None :
        return proxy
      else :
        return NodeProxy(url=descr["url"], id=node_id)



# NodeProxy object
#
# Acts like a local node, but all calls are remote.  If a node goes
# down, the connection will timeout, and a socket.timeout error will
# be thrown.
#
# Closing the connection doesn't seem to work though.
class NodeProxy(object) :

  # Class-level object to handle the translation between nodes
  # descriptions and actual nodes.  Class-level, so that the server
  # object can configure it to not return proxies for the node it's
  # serving.
  node_translator = ProxyTranslation()

  # Class level methods.  Left as remnant of code pre-refactoring.
  @classmethod
  def to_descr(cls, node) :
    return cls.node_translator.to_descr(node)

  @classmethod
  def from_descr(cls, descr) :
    return cls.node_translator.from_descr(descr)


  def __init__(self, url, id=None, timeout=5, verbose=True) :
    # Should parse the URL to makes sure it's http, or if not, add the protocol
    self.url = url
    self.server = TimeoutServerProxy(url, timeout=timeout, verbose=verbose,
                                     allow_none=True)
    self.__id = id
    self.logger = logging.getLogger("dyschord.nodeproxy")
    self.logger.debug("Created node proxy to url %s with id %s", url, id)

  @property
  def id(self) :
    if self.__id is None :
      ping = self.server.ping()
      self.__id = int(ping["id"])
    return self.__id

  def __repr__(self) :
    # Should add timeout and verbose arguments, but this is good enough for now
    rslt = 'NodeProxy("%s", id=%d)' % (self.url, self.id)
    return rslt


  def get_next(self) :
    next = self.server.get_next()
    return self.node_translator.from_descr(next)

  def set_next(self, value) :
    self.server.set_next(self.node_translator.to_descr(value))

  next = property(get_next, set_next, doc="Successor node")
  

  @property
  def predecessor(self) :
    predecessor = self.server.get_predecessor()
    return self.node_translator.from_descr(predecessor)

  def close(self) :
    self.server("close")()


  def store_backup(self, key, value, predecessor) :
    return self.server.store_backup(key, value,
                                    self.node_translator.to_descr(predecessor))

  def find_node(self, key_hash) :
    node_info = self.server.find_node(key_hash)
    self.logger.debug("Making new proxy for %s", node_info)
    return self.node_translator.from_descr(node_info)

  def __getattr__(self, attr) :
    # Maybe it's a method on the server...
    return getattr(self.server, attr)

  def closest_preceding_node(self, key_hash) :
    node_info = self.server.closest_preceding_node(key_hash)
    self.logger.debug("Closest node to %d is %s", key_hash, node_info)
    return self.node_translator.from_descr(node_info)

  def update_fingers_on_insert(self, node) :
    self.logger.debug("Sending request to update fingers to node %d", node.id)
    return self.server.update_fingers_on_insert(
      self.node_translator.to_descr(node))

  def prepend_node(self, node, url=None) :
    return self.server.prepend_node(self.node_translator.to_descr(node))

  def setup(self, predecessor, fingers, data) :
    self.server.setup(
      self.node_translator.to_descr(predecessor),
      dict((str(step), self.node_translator.to_descr(finger))
           for step, finger in fingers.iteritems()),
      data)

  def get_fingers(self) :
    fingers = self.server.get_fingers()
    fingers = dict((int(step), self.node_translator.from_descr(descr))
                   for step, descr in fingers.iteritems())
    return fingers

  def successor_leaving(self, new_successor) :
    self.server.successor_leaving(self.node_translator.to_descr(new_successor))

  def predecessor_leaving(self, new_predecessor,data) :
    self.server.predecessor_leaving(
      self.node_translator.to_descr(new_predecessor), data)

  def leave(self) :
    return self.server.leave()


# Non-peer client
class Client(object) :
  def __init__(self, peers, min_connections=3) :
    self.logger = logging.getLogger("dyschord.client")
    self.cloud = {}
    for p in peers :
      peer = NodeProxy(p)
      try :
        peer_id = peer.id
      except (socket.error, socket.timeout) :
        # Error connecting to node
        continue
      self.cloud[p] = peer
    if not self.cloud :
      raise Exception("Unable to connect to any nodes")
    self.min_connections = min_connections
    if len(self.cloud) < self.min_connections :
      self._find_connections()

  def _find_connections(self) :
    known_peers = list(self.cloud.values())
    while len(self.cloud) < self.min_connections and known_peers :
      peer = known_peers.pop(0)
      try :
        others = peer.get_fingers()
      except (socket.error, socket.timeout) :
        del self.cloud[peer.url]
        continue
      for finger in others.itervalues() :
        if finger.url not in self.cloud :
          self.cloud[finger.url] = finger
          known_peers.append(finger)
    if not self.cloud :
      raise Exception("Unable to connect to any peers")
    if len(self.cloud) < self.min_connections :
      self.logger.warn("Only aware of %d peers", len(self.cloud))


  def lookup(self, key) :
    """Lookup value for key

    parameters:
      - key       string"""
    # Note: keys are str not basestring, to avoid worrying about unicode issues
    if not isinstance(key, str) :
      raise Exception("Unable to handle nonstring key %s" % key)
    self._find_connections()
    for peer_id, peer in self.cloud.items() :
      try :
        rslt = peer.lookup(key)
      except (socket.error, socket.timeout) :
        # Error connecting to node
        del self.cloud[peer_id]
        continue
      except xmlrpclib.Fault, e :
        if e.faultCode == 404 :
          raise KeyError(e.faultString)
      else :
        return json.loads(rslt)
      break
    if not self.cloud :
      raise Exception("Unable to connect to any nodes")

  def store(self, key, value) :
    """Store value for key

    parameters:
      - key       string
      - value     a json-encodable object"""
    if not isinstance(key, str) :
      raise Exception("Unable to handle nonstring key %s" % key)
    self._find_connections()
    for peer_id, peer in self.cloud.items() :
      try :
        peer.store(key, json.dumps(value))
      except (socket.error, socket.timeout) :
        # Error connecting to node
        del self.cloud[peer_id]
        continue
      break
    if not self.cloud :
      raise Exception("Unable to connect to any nodes")
