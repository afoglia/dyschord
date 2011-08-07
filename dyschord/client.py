import xmlrpclib
import socket

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


# NodeProxy object
#
# Acts like a local node, but all calls are remote.  If a node goes
# down, the connection will timeout, and a socket.timeout error will
# be thrown.
#
# Closing the connection doesn't seem to work though.
class NodeProxy(object) :
  def __init__(self, url, id=None, timeout=2, verbose=False) :
    # Should parse the URL to makes sure it's http, or if not, add the protocol
    self.server = TimeoutServerProxy(url, timeout=timeout, verbose=verbose)
    self.__id = id

  @property
  def id(self) :
    if self.__id is not None :
      return self.__id
    else :
      ping = self.server.ping()
      self.__id = int(ping["id"])
    
  def close(self) :
    self.server("close")()

  def find_node(self, key_hash) :
    node_info = self.server.find_node(key_hash)
    return NodeProxy(node_info["url"], id=node_info.get("id"))

  def __getattr__(self, attr) :
    # Maybe it's a method on the server...
    return getattr(self.server, attr)


# Non-peer client
class Client(object) :
  def __init__(self, cloud) :
    self.cloud = {}
    for c in cloud :
      peer = NodeProxy(c)
      try :
        peer_id = peer.id
      except (socket.error, socket.timeout) :
        # Error connecting to node
        continue
      self.cloud[c] = peer
    if not self.cloud :
      raise Exception("Unable to connect to any nodes")

  def lookup(self, key) :
    for peer_id, peer in self.cloud.items() :
      try :
        return peer.lookup(key)
      except (socket.error, socket.timeout) :
        # Error connecting to node
        del self.cloud[peer_id]
        continue
      break
    if not self.cloud :
      raise Exception("Unable to connect to any nodes")

  def store(self, key, value) :
    for peer_id, peer in self.cloud.items() :
      try :
        peer.store(key, value)
      except (socket.error, socket.timeout) :
        # Error connecting to node
        del self.cloud[peer_id]
        continue
      break
    if not self.cloud :
      raise Exception("Unable to connect to any nodes")
