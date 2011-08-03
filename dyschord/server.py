#!/usr/bin/env python

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
from xmlrpclib import Binary
import datetime
import sys

from . import readwritelock
from . import node


# Threaded XML RPC Server
class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer) :
  """Threading XML-RPC Server"""

# Can derive from the Node class itself
class DyschordService(object) :
  def __init__(self) :
    self.node = node.Node()

  def ping(self) :
    return {"id": str(self.get_id())}

  def get_id(self) :
    return self.node.id

  def lookup(self, key) :    
    pass

  def find_successor(self, key_hash) :
    return self.node.find_successor(key_hash)

  def closest_preceding_node(self, key_hash) :
    return self.node.find_successor(key_hash)

  
  
def start(port) :
  # I don't know what the "localhost" part of the server address is
  # for.  It looks like it's used to bind the socket, so it should
  # always be localhost.  (Unless it can be used to bind to just
  # interface of a system with multiple ip addresses?)
  server = ThreadedXMLRPCServer(("localhost", port),
                                logRequests=True,
                                allow_none=True)
  server.register_introspection_functions()
  server.register_multicall_functions()

  server.register_instance(DyschordService())

  try :
    print "Use Contrl-C to exit"
    server.serve_forever()
  except KeyboardInterrupt :
    print "Exiting"

import optparse

def main(args=sys.argv) :
  parser = optparse.parser("%prog [OPTIONS]")
  parser.add_option("-p", "--port", default=65000)
  options, args = parser.parse_args(args)
  start(options.port)
