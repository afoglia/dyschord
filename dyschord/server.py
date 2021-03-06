#!/usr/bin/env python

from SimpleXMLRPCServer import SimpleXMLRPCServer, Fault
from SocketServer import ThreadingMixIn
from xmlrpclib import Binary
import datetime
import sys
import json
import threading
import socket
import logging
import logging.config
import time
import optparse


from . import readwritelock
from . import node as core
from .client import NodeProxy

# Threaded XML RPC Server
class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer) :
  """Threading XML-RPC Server"""


class DyschordService(object) :
  """DyschordService handles incoming XML-RPC calls and translates for a node"""
  def __init__(self, mynode) :
    self.node = mynode
    self.url = None
    self.logger = logging.getLogger("dyschord.service")

  def ping(self) :
    return {"id": str(self.get_id())}

  def get_id(self) :
    return self.node.id

  def lookup(self, key) :
    key_hash = self.node.hash_key(key)
    ntries = 2
    while ntries > 0 :
      ntries -= 1
      try :
        if (self.node.distance(key_hash, self.node.id)
            <= self.node.distance(key_hash, self.node.predecessor.id)) :
          return self.node[key]
      except (socket.error, socket.timeout) :
        if ntries == 0 :
          self.logger.error("Node pointer corruption!!!")
        self.node.repair_predecessor()
        self.node.repair_fingers()
      except KeyError, e :
        raise Fault(404, e.message)
      try :
        target_node = core.find_node(self.node, key_hash)
      except (socket.error, socket.timeout) :
        if ntries == 0 :
          self.logger.error("Node pointer corruption!!!")
        self.repair_predecessor()
        self.repair_fingers()
    return target_node.lookup(key)

  def repair_fingers(self) :
    self.node.repair_fingers()

  def repair_predecessor(self) :
    self.node.repair_predecessor()

  def store(self, key, value) :
    key_hash = self.node.hash_key(key)
    if (self.node.distance(key_hash, self.node.id)
        <= self.node.distance(key_hash, self.node.predecessor.id)) :
      self.node[key] = value
      return
    target_node = core.find_node(self.node, key_hash)
    target_node.store(key, value)

  def store_backup(self, key, value, predecessor) :
    self.logger.debug("Storing backup of key-value (%s, %s)", key, value)
    self.node.store_backup(key, value,
                                  self._node_from_descr(predecessor))

  def update_backup(self, data) :
    self.node.update_backup(data)

  def _serialize_node_descr(self, node) :
    return NodeProxy.to_descr(node)

  def _node_from_descr(self, descr) :
    # Translate node descriptions into either proxy nodes, or
    # references to the wrapped node.
    return NodeProxy.from_descr(descr)

  def find_successor(self, key_hash) :
    rslt = core.find_node(self.node, key_hash)
    return self._serialize_node_descr(rslt)

  def closest_preceding_node(self, key_hash) :
    ntries = 2
    while ntries > 0 :
      ntries -= 1
      try :
        return NodeProxy.node_translator.to_descr(
          self.node.closest_preceding_node(key_hash))
      except (socket.error, socket.timeout) :
        if ntries == 0 :
          self.logger.error("Node pointer corruption!!!")
          raise
        self.node.repair_predecessor()
        self.node.repair_fingers()

  def get_next(self) :
    rslt = self.node.next
    return self._serialize_node_descr(rslt)

  def set_next(self, value) :
    self.node.next = NodeProxy.from_descr(value)

  def get_predecessor(self) :
    rslt = self.node.predecessor
    return self._serialize_node_descr(rslt)

  def update_fingers_on_insert(self, node) :
    self.logger.debug("Received request to update fingers for new node %s",
                      node.get("id"))
    return self.node.update_fingers_on_insert(NodeProxy.from_descr(node))

  def update_fingers_on_leave(self, leaving, successor_of_leaving) :
    return self.node.update_fingers_on_leave(
      NodeProxy.node_translator.from_descr(leaving),
      NodeProxy.node_translator.from_descr(successor_of_leaving))


  def prepend_node(self, node_descr) :
    node_proxy = self._node_from_descr(node_descr)
    self.logger.debug("Trying to prepend node %d", node_proxy.id)
    self.node.prepend_node(node_proxy)
    self.logger.debug("Successfully prepended node")

  def setup(self, predecessor, fingers, data) :
    self.logger.debug(
      "Setup called with predecessor %s, fingers %s, and data %s",
      predecessor, fingers, data)
    self.node.setup(
      self._node_from_descr(predecessor),
      dict((int(step), self._node_from_descr(finger))
           for step, finger in fingers.iteritems()),
      data)
    self.logger.debug("Successfully setup node")

  def get_fingers(self) :
    # Note keys of dictionaries passed through XML-RPC must be strings
    finger_dict = dict(
      (str(step), self._serialize_node_descr(finger))
       for step, finger in self.node.get_fingers().iteritems())
    return finger_dict

  def successor_leaving(self, new_successor) :
    self.node.successor_leaving(self._node_from_descr(new_successor))

  def predecessor_leaving(self, new_predecessor, data) :
    self.node.predecessor_leaving(self._node_from_descr(new_predecessor), data)

  def leave() :
    self.logger.info("Shutting down")
    self.node.leave()


class PredecessorMonitor(threading.Thread) :
  def __init__(self, node, heartbeat=10) :
    self.node = node
    self.frequency = heartbeat
    self.logger = logging.getLogger("dyschord.service.link_monitor")
    self._stop_event = threading.Event()
    # By using a Condition I can stop this thread even if it's
    # sleeping.  Not necessary for here, but I wanted to see if this
    # technique works, and it does.
    self._wakeup = threading.Condition()

  def run(self) :
    with self._wakeup :
      while not self._stop_event.is_set() :
        self.logger.debug("Checking predecessor")
        predecessor = self.node.predecessor
        try :
          predecessor.ping()
        except (socket.error, socket.timeout) :
          self.logger.warn("Predecessor %d at %s non-responsive",
                           predecessor.id, predecessor.url)
          self.node.repair_predecessor()
          new_pred = self.node.predecessor
          self.logger.info(
            "Replacing old precessor with new predecessor %d at %s",
            new_pred.id, new_pred.url)
          new_pred.successor_leaving(self.node)
        self._wakeup.wait(self.frequency)

  def stop(self) :
    self._stop_event.set()
    with self._wakeup :
      self._wakeup.notify()


def start_in_thread(server) :
  server_main_thread = threading.Thread(target=server.serve_forever)
  server_main_thread.start()
  return server_main_thread

  
  
def start(port, node=None, cloud_addrs=[], heartbeat=10,
          log_requests=False, forever=True) :
  if node is None :
    node = core.Node()
  service = DyschordService(node)
  service.url = "http://localhost:%d" % port
  service.node.url = service.url
  NodeProxy.node_translator.url = service.url
  NodeProxy.node_translator.local_nodes[node.id] = node

  server = ThreadedXMLRPCServer(("localhost", port),
                                logRequests=log_requests,
                                allow_none=True)
  server.register_introspection_functions()
  server.register_multicall_functions()
  server.register_instance(service)

  server_thread = None
  pred_monitor = None
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
      try :
        print "Connectiong to node %d at %s" % (successor.id, successor.url)
        successor.prepend_node(service.node)
      except (socket.timeout, socket.error) :
        # Node might have went down while trying to connect.  Try another.
        print ("Unable to connect to node %d @ %s"
               % (successor.id, successor.url))
        continue
      break
    
    else :
      print "Unable to find other nodes to join"

    # All initialized
    service.logger.info("Successfully setup node")
    service.node.initialized = True

    # Kick off another thread to monitor the successor and make sure
    # that's always correct...
    pred_monitor = PredecessorMonitor(service.node, heartbeat=heartbeat)
    pred_monitor.run()
    while forever :
      time.sleep(60)
  except KeyboardInterrupt :
    forever = True
    print "Exiting"
  except Exception, e :
    print "Fatal error:", e
    logging.exception("Fatal error while setting up server:")
    raise
  finally :
    if forever :
      if pred_monitor is not None :
        logging.debug("Shutting down predecessor monitor")
        pred_monitor.stop()
      service.node.leave()
      server.shutdown()
      if server_thread is not None :
        server_thread.join()

  return service, server_thread


def main(args=sys.argv) :
  parser = optparse.OptionParser("%prog [OPTIONS] OTHER_NODES")
  parser.add_option("--conf", default="dyschord.conf",
                    help="Config file [default: %default]")
  parser.add_option("-p", "--port", type=int)
  parser.add_option("--id", type=int,
                    help="Id value of node")
  parser.add_option("--log-config", dest="log_config",
                    help="Logging configuration ini file")
  parser.add_option("--log-requests", dest="log_requests",
                    action="store_true",
                    help="Turn on request logging in the XML-RPC server")
  parser.add_option("--proxy-verbose", dest="proxy_verbose",
                    action="store_true",
                    help="Verbose output from XML-RPC clients")
  options, args = parser.parse_args(args)

  try :
    config = json.load(open(options.conf))
  except Exception, e :
    sys.stderr.write("Unable to parse config file %s\n" % options.conf)
    sys.stderr.write("Exception: %s\n" % e)
    sys.exit(1)

  if options.log_config :
    logging.config.fileConfig(options.log_config)

  if options.port is not None :
    config["port"] = options.port
  if options.id is not None :
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

  if options.log_requests is not None :
    config["log_requests"] = options.log_requests

  if options.proxy_verbose is not None :
    config["proxy_verbose"] = options.proxy_verbose

  NodeProxy.verbose = config.get("proxy_verbose", False)

  node = core.Node(config.get("node_id"), metric=metric)

  start(config.get("port", 10000), node,
        cloud_addrs=config.get("cloud_members", []),
        heartbeat=config.get("heartbeat", 10),
        log_requests=config.get("log_requests", False))


if __name__=="__main__" :
  main()
