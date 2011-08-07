#!/usr/bin/env python

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
from xmlrpclib import Binary
import datetime
import sys
import json
import threading
import socket

from . import readwritelock
from . import node as core
from .client import NodeProxy

# Threaded XML RPC Server
class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer) :
  """Threading XML-RPC Server"""

# Eventually, I hope to derive from the Node class itself
class DyschordService(core.DistributedHash) :
  def __init__(self, mynode) :
    self.node = mynode
    core.DistributedHash.__init__(self, self.node)

  def ping(self) :
    return {"id": str(self.get_id())}

  def get_id(self) :
    return self.node.id

  def lookup(self, key) :
    key_hash = self.node.hash_key(key)
    if (self.node.distance(key_hash, self.node.id)
        <= self.node.distance(key_hash, self.node.predecessor.id)) :
      return self.node[key]
    target_node = core.find_node(self.node, key_hash)
    return target_node.lookup(key)

  def store(self, key, value) :
    key_hash = self.node.hash_key(key)
    if (self.node.distance(key_hash, self.node.id)
        <= self.node.distance(key_hash, self.node.predecessor.id)) :
      self.node[key] = value
      return
    target_node = core.find_node(self.node, key_hash)
    target_node.store(key, value)

  def find_successor(self, key_hash) :
    return self.node.find_successor(key_hash)

  def closest_preceding_node(self, key_hash) :
    rslt = self.node.closest_preceding_node(key_hash)
    return {"id":rslt.id, "url":getattr(rslt, "url", None)}

  
def start_in_thread(server) :
  server_main_thread = threading.Thread(target=server.serve_forever)
  server_main_thread.start()
  return server_main_thread

  
  
def start(port, node=None, cloud_addrs=[], forever=True) :
  # I don't know what the "localhost" part of the server address is
  # for.  It looks like it's used to bind the socket, so it should
  # always be localhost.  (Unless it can be used to bind to just
  # interface of a system with multiple ip addresses?)
  if node is None :
    node = core.Node()
  server = ThreadedXMLRPCServer(("localhost", port),
                                logRequests=True,
                                allow_none=True)
  server.register_introspection_functions()
  server.register_multicall_functions()

  service = DyschordService(node)
  server.register_instance(service)

  server_thread = None
  try :
    print "Starting service on port", port
    print "Use Contrl-C to exit"
    server_thread = start_in_thread(server)
    for cloud_addr in cloud_addrs :
      # Simple check so I can use the same configuration file for
      # multiple test servers.
      if cloud_addr == "http://localhost:%d" % port :
        continue
      neighbor = NodeProxy(cloud_addr)
      try :
        neighbor.ping()
      except (socket.timeout, socket.error) :
        # Node down.  Try next in list
        continue
      try :
        successor = core.find_node(neighbor, node.id)
      except (socket.timeout, socket.error) :
        # A node in the chain is down, although I will make the
        # individual nodes smart enough to work around that...  The
        # problem is, if this fails on one iteration, it should fail
        # on all, unless there is a problem with chords, and the node
        # cloud has split in two.
        continue

      # What if this fails?  How could it fail?
      successor.prepend_node(service.node)
      break
    
    else :
      print "Unable to find other nodes to join"

    # All initialized
    service.node.initialized = True
    while forever :
      pass
  except KeyboardInterrupt :
    server.shutdown()
    if server_thread is not None :
      server_thread.join()
    print "Exiting"

  return service, server_thread


import optparse

def main(args=sys.argv) :
  parser = optparse.OptionParser("%prog [OPTIONS] OTHER_NODES")
  parser.add_option("--conf", default="dyschord.conf",
                    help="Config file [default: %default]")
  parser.add_option("-p", "--port", type=int)
  parser.add_option("--id", type=int,
                    help="Id value of node")
  options, args = parser.parse_args(args)
  try :
    config = json.load(open(options.conf))
  except Exception, e :
    sys.stderr.write("Unable to parse config file %s\n" % options.conf)
    sys.stderr.write("Exception: %s\n" % e)
    sys.exit(1)

  if options.port :
    config["port"] = options.port
  if options.id :
    config["node_id"] = options.id

  node = core.Node(config.get("node_id"))

  start(config.get("port", 10000), node,
        cloud_addrs=config.get("cloud_members", []))


if __name__=="__main__" :
  main()
