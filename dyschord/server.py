#!/usr/bin/env python

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
from xmlrpclib import Binary
import datetime
import sys
import json
import threading
import socket
import logging
import logging.config

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
    self.url = None
    self.logger = logging.getLogger("dyschord.service")

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

  def _serialize_node_descr(self, node) :
    return {"id": node.id, "url": getattr(node, "url", self.url)}

  def find_successor(self, key_hash) :
    rslt = core.find_node(self.node, key_hash)
    return self._serialize_node_descr(rslt)

  def closest_preceding_node(self, key_hash) :
    rslt = self.node.closest_preceding_node(key_hash)
    return self._serialize_node_descr(rslt)

  def get_next(self) :
    rslt = self.node.next
    return self._serialize_node_descr(rslt)

  def get_predecessor(self) :
    rslt = self.node.predecessor
    return self._serialize_node_descr(rslt)

  def prepend_node(self, node_descr) :
    node_proxy = NodeProxy.from_descr(node_descr)
    self.logger.debug("Trying to prepend node %d", node_proxy.id)
    return self.node.prepend_node(node_proxy)

  def setup(self, predecessor, fingers, data) :
    self.logger.debug(
      "Setup called with predecessor %s, fingers %s, and data %s",
      predecessor, fingers, data)
    return self.node.setup(NodeProxy.from_descr(predecessor),
                           [NodeProxy.from_descr(finger) for finger in fingers],
                           data)

  def get_fingers(self) :
    # Note keys of dictionaries passed through XML-RPC must be strings
    finger_dict = dict(
      (str(step), self._serialize_node_descr(finger))
       for step, finger in zip(self.node.finger_steps, self.node.fingers))
    return finger_dict



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
  service.url = "http://localhost:%d" % port
  service.node.url = service.url
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
      successor.prepend_node(service.node, url=service.url)
      break
    
    else :
      print "Unable to find other nodes to join"

    # All initialized
    service.node.initialized = True
    while forever :
      pass
  except KeyboardInterrupt :
    forever = True
    print "Exiting"
  finally :
    if forever :
      server.shutdown()
      if server_thread is not None :
        server_thread.join()

  return service, server_thread


import optparse

def main(args=sys.argv) :
  parser = optparse.OptionParser("%prog [OPTIONS] OTHER_NODES")
  parser.add_option("--conf", default="dyschord.conf",
                    help="Config file [default: %default]")
  parser.add_option("-p", "--port", type=int)
  parser.add_option("--id", type=int,
                    help="Id value of node")
  parser.add_option("--log-config", dest="log_config",
                    help="Logging configuration ini file")
  options, args = parser.parse_args(args)

  try :
    config = json.load(open(options.conf))
  except Exception, e :
    sys.stderr.write("Unable to parse config file %s\n" % options.conf)
    sys.stderr.write("Exception: %s\n" % e)
    sys.exit(1)

  if options.log_config :
    logging.config.fileConfig(options.log_config)

  if options.port :
    config["port"] = options.port
  if options.id :
    config["node_id"] = options.id

  metric_name = config.get("metric")
  if metric_name is not None :
    metric_name = metric_name.lower()
  else :
    metric_name = "md5"
  config["metric"] = metric_name

  if metric_name == "md5" :
    metric = core.Md5Metric()
  elif metric_name == "trivial" :
    metric = core.TrivialMetric(4)
  else :
    raise Exception('Unrecognized metric "%s"' % metric_name)

  node = core.Node(config.get("node_id"), metric=metric)

  start(config.get("port", 10000), node,
        cloud_addrs=config.get("cloud_members", []))


if __name__=="__main__" :
  main()
