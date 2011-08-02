#!/usr/bin/env python

# Example of xmlrpclib library from The Python Standard Library by
# Example, Sections 12.10 and 12.11

from __future__ import with_statement

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SocketServer import ThreadingMixIn
from xmlrpclib import Binary
import datetime

from readwritelock import RWLock

# Threaded XML RPC Server
class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer) :
  """Threading XML-RPC Server"""


server = ThreadedXMLRPCServer(("localhost", 9000),
                              logRequests=True,
                              allow_none=True)
server.register_introspection_functions()
server.register_multicall_functions()

import time

class ExampleService:
  def __init__(self) :
    self.data = {'a': 0, 'b': 1}
    self._lock = RWLock()

  def store(self, key, value, wait) :
    with self._lock.wrlocked() :
      print "Got request to append:", value
      time.sleep(wait)
      self.data[key] = value

  def list(self, delay) :
    with self._lock.rdlocked() :
      print "Returning values:"
      rslt = []
      for k in self.data :
        rslt.append(k)
        time.sleep(delay)
      return rslt

  def ping(self) :
    """Simple function to respond when called to demonstrate connectivity."""
    return True

  def now(self) :
    """Returns the server current date and time."""
    return datetime.datetime.now()

  def show_type(self, arg) :
    """Illustrates how types are passed in and out of server methods.

    Accepts one argument of any type.
    Returns a tuple with the string representation of the value, the
    name of the type, and the value itself."""
    return (str(arg), str(type(arg)), arg)

  def raises_exception(self, msg) :
    """Always raises a RuntimeError with the message passed in"""
    raise RuntimeError(msg)

  def send_back_binary(self, bin) :
    """Accepts a single Binary argument, unpacks and repacks it to return it."""
    data = bin.data
    response = Binary(data)
    return response

server.register_instance(ExampleService())

try :
  print "Use Control-C to exit"
  server.serve_forever()
except KeyboardInterrupt :
  print "Exiting"


  