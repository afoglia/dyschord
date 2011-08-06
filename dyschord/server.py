#!/usr/bin/env python

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
from xmlrpclib import Binary
import datetime
import sys
import json
import threading

from . import readwritelock
from . import node
from .client import NodeProxy

# Threaded XML RPC Server
class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer) :
  """Threading XML-RPC Server"""

# Can derive from the Node class itself
class DyschordService(node.DistributedHash) :
  def __init__(self, id=None) :
    self.node = node.Node(id)
    node.DistributedHash.__init__(self, self.node)

  def ping(self) :
    return {"id": str(self.get_id())}

  def get_id(self) :
    return self.node.id

  def lookup(self, key) :
    key_hash = self.node.hash_key(key)
    if (self.node.distance(key_hash, self.node.id)
        <= self.node.distance(key_hash, self.node.predecessor.id)) :
      return self.node[key]
    target_node = node.find_node(self.node, key_hash)
    return target_node.lookup(key)

  def store(self, key, value) :
    pass

  def find_successor(self, key_hash) :
    return self.node.find_successor(key_hash)

  def closest_preceding_node(self, key_hash) :
    rslt = self.node.closest_preceding_node(key_hash)
    return {"id":rslt.id, "url":getattr(rslt, "url", None)}

  
def start_in_thread(server) :
  server_main_thread = threading.Thread(target=server.serve_forever)
  server_main_thread.start()
  return server_main_thread

  
  
def start(port, nodeid=None, cloud_addrs=[]) :
  # I don't know what the "localhost" part of the server address is
  # for.  It looks like it's used to bind the socket, so it should
  # always be localhost.  (Unless it can be used to bind to just
  # interface of a system with multiple ip addresses?)
  print "Starting service on port", port
  server = ThreadedXMLRPCServer(("localhost", port),
                                logRequests=True,
                                allow_none=True)
  server.register_introspection_functions()
  server.register_multicall_functions()

  service = DyschordService()
  server.register_instance(service)

  print "Use Contrl-C to exit"
  server_thread = start_in_thread(server)
  try :
    for cloud_addr in cloud_addrs :
      neighbor = NodeProxy(cloud_addr)
      try :
        neighbor.ping()
      except (socket.timeout, socket.error) :
        # Node down.  Try next in list
        continue
      try :
        successor = node.find_node(neighbor, service.id)
      except (socket.timeout, socket.error) :
        # A node in the chain is down, although I will make the
        # individual nodes smart enough to work around that...  The
        # problem is, if this fails on one iteration, it should fail
        # on all, unless there is a problem with chords, and the node
        # cloud has split in two.
        continue

      # What if this fails?  How could it fail?
      successor.prepend_node(node)
      break
    
    else :
      print "Unable to find other nodes to join"
    # All initialized
    node.initialized = True
    while True :
      pass
  except KeyboardInterrupt :
    server.shutdown()
    if server_thread is not None :
      server_thread.join()
    print "Exiting"

import optparse

def main(args=sys.argv) :
  parser = optparse.OptionParser("%prog [OPTIONS] OTHER_NODES")
  parser.add_option("--conf", default="dyschord.conf",
                    help="Config file [default: %default]")
  parser.add_option("-p", "--port", type=int)
  options, args = parser.parse_args(args)
  try :
    config = json.load(open(options.conf))
  except Exception, e :
    sys.stderr.write("Unable to parse config file %s\n" % options.conf)
    sys.stderr.write("Exception: %s\n" % e)
    sys.exit(1)

  if options.port :
    config["port"] = options.port
  start(config.get("port", 10000), config.get("node_id"),
        cloud_addrs=config.get("cloud_members", []))


if __name__=="__main__" :
  main()
