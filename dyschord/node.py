# Heavily based on example at <http://www.linuxjournal.com/article/6797>
import hashlib
import uuid
import functools
import bisect
from collections import MutableMapping
import logging

from . import readwritelock

_logger = logging.getLogger("dyschord.core")

class Md5Metric(object) :
  # Since both the md5 and uuid are 128-bit, I can use the former for
  # hashing, and the latter for the node ids and the sizes already work.

  def __init__(self, hash_bits=128) :
    self.hash_bits = hash_bits

  # I don't want to shadow a builtin, so I'll give it this clumsy name.
  def hash_key(self, key) :
    """Hash function for finding the appropriate node"""
    return int(hashlib.md5(key).hexdigest(), 16) % 2**self.hash_bits


  # Clockwise ring function taken from
  # <http://www.linuxjournal.com/article/6797>
  def distance(self, a, b) :
    return (b-a) % 2**self.hash_bits
    # # k is the number of bits in the ids used.  For a uuid, this is 128
    # # bytes.  If this were C, one would need to worry about overflow,
    # # because 2**128 takes 129 bytes to store.
    # if a < b:
    #   return b-a
    # elif a == b :
    #   return 0
    # else :
    #   return (2**self.hash_bits) + (b-a)

class TrivialMetric(Md5Metric) :
  def __init__(self, hash_bits) :
    Md5Metric.__init__(self, hash_bits)

  def hash_key(self, key) :
    return int(key) % 2**self.hash_bits



# Keep the finger_table_size small for testing, so I can get my head around it
finger_table_size = 128


def compute_finger_steps(hash_bits, finger_table_size) :
  finger_table_size = min(finger_table_size, hash_bits)
  return sorted(2**(int((hash_bits)*i*1.0/finger_table_size))
                for i in xrange(finger_table_size))



# Node finding function taken from <http://www.linuxjournal.com/article/6797>
def find_predecessor(start, key_hash) :
  _logger.debug("Finding predecessor for %d starting at node %d",
                key_hash, start.id)
  current = start
  while True :
    next = current.closest_preceding_node(key_hash)
    if next.id == current.id :
      break
    else :
      current = next
  return current


# Non-finger-based lookup logic, so I can replace the new logic with
# the old for timing purposes.
def find_predecessor_without_fingers(start, key_hash) :
  current = start
  distance = current.distance
  while True :
    if (distance(key_hash, current.id)
        < distance(key_hash, current.next.id)) :
      current = current.next
    else :
      break
  return current

def find_node(start, key_hash) :
  rslt = find_predecessor(start, key_hash)
  return rslt.next


class Uninitialized(Exception) :
  pass


# Helper function to deactivate methods while the node is starting up.
# The other options are:
#
# 1. Instead of wrapping the methods I could dynamically add them, but
# then I they won't have been parsed by the XML-RPC server (assuming
# it does the scan of methods only once).  Plus, that's more
# complicated.  But it would have the benefit of having no cost once
# the node is initializaed.
#
# 2. Use a lock.  But I don't want to block, I want to throw an exception.  And I don't want have to pay the overhead of the extra lock.
def initialization_check(wrappee) :
  @functools.wraps(wrappee)
  def wrapped(*args, **kwargs) :
    self=args[0]
    if not args[0].initialized :
      raise Uninitialized("Node is still starting up")
    return wrappee(*args, **kwargs)
  return wrapped


# I could probably derive from a dictionary, and just add extra
# properties and methods, but I might need to change too many
# functions, especially when I want to persist the data to disk.
class Node(MutableMapping) :

  def __init__(self, id=None, nfingers=None, metric=None) :
    # nfingers   None means default

    # uuid4 is not uniform over 2**128 because hex digit 13 is always
    # 4, and hex digit 17 is either 8, 9, A, or B.  But since these
    # are low digits, it shouldn't matter unless the number of nodes
    # becomes super-large (~2**32 or so).  Plus using uuid4 means that
    # the code will run on both Linux and Windows (otherwise, I'd have
    # to copy the logic used in uuid.uuid4
    if id is None :
      self.__uuid = uuid.uuid4()
    else :
      self.__uuid = uuid.UUID(int=id)
    self.__metric = metric if metric else Md5Metric()
    self.__id = self.__uuid.int % 2**self.__metric.hash_bits
    self.data = {}
    self.predecessor = self
    if not nfingers :
      nfingers = finger_table_size
    self.finger_steps = compute_finger_steps(
      self.__metric.hash_bits, nfingers)
    self.fingers = [self for f in self.finger_steps]
    self.initialized = False
    self.logger = _logger.getChild("Node")
    self.data_lock = readwritelock.RWLock()
    self.finger_lock = readwritelock.RWLock()

  @property
  def distance(self) :
    return self.__metric.distance

  @property
  def hash_key(self) :
    return self.__metric.hash_key

  @property
  def next(self) :
    return self.fingers[0]

  @property
  def id(self) :
    return self.__id

  @property
  def name(self) :
    return str(self.__uuid)

  def __str__(self) :
    return "Node(id=%d)" % self.id

  @initialization_check
  def __getitem__(self, key) :
    with self.data_lock.rdlocked() :
      return self.data[key]

  @initialization_check
  def __setitem__(self, key, value) :
    with self.data_lock.wrlocked() :
      self.data[key] = value

  @initialization_check
  def __delitem__(self, key) :
    with self.data_lock.wrlocked() :
      del self.data[key]

  @initialization_check
  def iterkeys(self) :
    # Locking this is not easy, and would require a custom generator.
    # Instead I won't bother and fall back on the same failure cases
    # that a normal python dictionary has (e.g. iterators throw
    # exceptions if the keys of the underlying dict have changes.)
    return self.data.iterkeys()
  __iter__ = iterkeys

  @initialization_check
  def keys(self) :
    with self.data_lock.rdlocked() :
      return self.data.iterkeys()

  @initialization_check
  def __contains__(self, key) :
    with self.data_lock.rdlocked() :
      return key in self.data

  @initialization_check
  def __len__(self) :
    with self.data_lock.rdlocked() :
      return len(self.data)

  # def find_successor(self, key_hash) :
  #   if distance(self.id, key_hash) >= distance(self.predecessor.id, key_hash) :
  #     return self
  #   else :
  #     return self.closest_preceding_node(key_hash).find_successor(key_hash)

  @initialization_check
  def closest_preceding_node(self, key_hash) :
    with self.finger_lock.rdlocked() :
      # If I were sure the metric was going to be the "clockwise"
      # distance, then I could use distance_to_node =
      # -distance_from_node % 2**hash_bits, but I want to keep the
      # flexibility and clarity in case I try a different topology.
      distance = self.distance
      distance_from_node = distance(self.id, key_hash)
      self.logger.log(5, "Distance from node: %d", distance_from_node)
      if distance_from_node == 0 :
        return self.predecessor
      distance_to_node = distance(key_hash, self.id)
      for finger_step, finger in \
            reversed(zip(self.finger_steps, self.fingers)) :
        self.logger.log(5, "Finger distance: %d", distance(key_hash, finger.id))
        if finger.id == key_hash :
          return finger.predecessor
        if finger_step >= distance_from_node :
          continue
        if (distance_to_node < distance(key_hash, finger.id)) :
          self.logger.log(5, "Advancing to finger %d", finger.id)
          return finger
      return self

  def update_fingers(self) :
    with self.finger_lock.wrlocked() :
      for i, step in enumerate(self.finger_steps) :
        old = self.fingers[i]
        self.fingers[i] = find_node(old, ((self.id+step)
                                          % 2**self.__metric.hash_bits))

  def update_fingers_on_insert(self, newnode) :
    with self.finger_lock.wrlocked() :
      # Faster updating when new node is added.
      self.logger.debug("Updating fingers on node %d for new node %d",
                        self.id, newnode.id)
      for i, step in enumerate(self.finger_steps) :
        old_finger = self.fingers[i]
        if old_finger.id == newnode.id :
          # Already registered.  Probably set to next during joining
          continue
        if (old_finger.id != self.id
            and (self.distance(self.id, old_finger.id)
                 < self.distance(self.id, newnode.id))) :
          continue
        self.logger.log(5, "Updating finger %d pointing %d away", i, step)
        self.fingers[i] = find_node(old_finger, ((self.id+step)
                                                 % 2**self.__metric.hash_bits))
        if self.fingers[i].id == old_finger.id :
          break

  @initialization_check
  def prepend_node(self, newnode) :
    # By making the method a "prepend node" called on the new
    # successor, I can reduce the traffic by combining the data and
    # fingers for the new node.  More importantly, I can lock the data
    # so any incoming stores for data that would no longer be in the
    # successor's control is blocked.

    # Ensure the node is correct
    with self.finger_lock.wrlocked() :
      successor = self
      predecessor = self.predecessor
      if predecessor is None :
        # Must be first joining...
        predecessor = self
      if self.id == newnode.id :
        raise Exception("Preexisting node with id")
      distance_to_newnode = self.distance(self.id, newnode.id)
      distance_to_predecessor = self.distance(self.id, predecessor.id)
      if distance_to_newnode < distance_to_predecessor :
        raise Exception("Nodes must be attached to their successor")
      if distance_to_newnode == distance_to_predecessor :
        # Should ping the successor to make sure it's still up.
        raise Exception("Preexisting node with id")

      self.logger.debug("Preparing data to send")
      with self.data_lock.wrlocked() :
        # Setup new node
        delegated_data = {}
        for k, v in successor.iteritems() :
          if (self.distance(self.hash_key(k), newnode.id)
              < self.distance(self.hash_key(k), successor.id)) :
            delegated_data[k] = v
        self.logger.debug("Sending data: %s", delegated_data)
        newnode.setup(predecessor, list(predecessor.fingers), delegated_data)

        # Establish new fingers to bring the new node into chain
        predecessor.fingers[0] = newnode
        successor.predecessor = newnode

        for k in delegated_data :
          # Should move to a backup, should I have time to establish that.
          del self.data[k]


  def setup(self, predecessor, fingers, data) :
    with self.finger_lock.wrlocked() :
      self.logger.debug("Setting up node with predecessor: %s", predecessor.id)
      self.predecessor = predecessor
      self.logger.debug("Setting up node with initial fingers: %s",
                        [(finger.id, getattr(finger, "url", None))
                         for finger in fingers])
      self.fingers = fingers
    with self.data_lock.wrlocked() :
      self.logger.debug("Setting up node with data: %s", data)
      self.data.update(data)
    self.initialized = True



def walk(start) :
  seen = set()
  node = start
  while True :
    if node.id in seen :
      raise Exception("Infinite loop.  Seen %s twice" % node.id)
    seen.add(node.id)
    yield node
    node = node.next
    if node.id == start.id :
      break


# Update fingers of other nodes.
#
# (a) only nodes from new_node.id - max(finger_step) to predecessor
# can possible have changes
#
# (b) for each node, only fingers that are from 1 to (new_node._id
# - node._id) need to change.
def announce(new_node) :
  for node in walk(new_node.next) :
    if node.id == new_node.id :
      break
    node.update_fingers_on_insert(new_node)


class DistributedHash(object) :
  def __init__(self, start=None) :
    # Start points to the "beginning" of the list
    self.__start = None
    if start is not None :
      self.join(start)

  def lookup(self, key) :
    # Can't just use hash because that might differ between Python
    # implementations.  Plus by using a different hash function, we
    # reduce the changes of collisions in the dictionaries in each node
    # assuming the algorithms are sufficiently different.  And Python's
    # hashing algorithm is relatively simple, so that's probably the
    # case.
    key_hash = self.__start.hash_key(key)
    node = find_node(self.__start, key_hash)
    return node.data[key]

  def store(self, key, value) :
    key_hash = self.__start.hash_key(key)
    node = find_node(self.__start, key_hash)
    node[key] = value


  def delete(self, key) :
    key_hash = self.__start.hash_key(key)
    node = find_node(self.__start, key_hash)
    del node[key]

  def _iternodes(self, start=None) :
    if self.__start is None :
      return []
    return walk(self.__start)

  def iterkeys(self) :
    for node in self._iternodes() :
      for k in node.iterkeys() :
        yield k

  def __len__(self) :
    return sum(len(node) for node in self._iternodes())

  def clear(self) :
    for node in self._iternodes() :
      node.clear()

  def num_nodes(self) :
    return len(list(self._iternodes()))

  def join(self, newnode) :
    # Base case: First node
    if self.__start is None :
      self.__start = newnode
      self.__start.fingers = [newnode]*len(self.__start.fingers)
      self.__start.initialized = True
      return

    predecessor = find_predecessor(self.__start, newnode.id)
    successor = predecessor.next
    if successor.id == newnode.id :
      raise Exception("Node already exists with same id")
    successor.prepend_node(newnode)

    # Optimize fingers.  Don't need to lock the nodes while this is
    # being done.
    newnode.update_fingers()
    announce(newnode)


  def leave(self, node) :
    if self.num_nodes() == 0 :
      return

    # Note I am looking up, because then I can get a node to leave by
    # passing in another instance with the same id.  Might be useful
    # for testing.
    predecessor = find_predecessor(self.__start, node.id)
    # print "Leaving node %d has predecessor %d" % (node.id, predecessor.id)
    if predecessor.next.id != node.id :
      # No joined node with this id.  Maybe log the missing node, but
      # work is done
      return
    
    leaving = predecessor.next
    successor = leaving.next

    # Check we aren't removing first item
    if leaving.id == self.__start.id :
      self.__start = successor

    # Copy data from leaving node
    successor.update(leaving)

    # Can update this by not call update fingers on all nodes, but
    # only on those that had the leaving node.  In fact, I don't think
    # I need to update the fingers at all, other than replacing all
    # fingers to the leaving to the sucessor.
    for remaining_node in self._iternodes() :
      if remaining_node is not leaving :
        remaining_node.fingers = [
          finger if finger.id != leaving.id else successor
          for finger in remaining_node.fingers]

    successor.predecessor = predecessor
    for remaining_node in self._iternodes() :
      remaining_node.update_fingers()
