# Heavily based on example at <http://www.linuxjournal.com/article/6797>
import hashlib
import uuid
import functools
import bisect
from collections import MutableMapping
import logging
import socket
import itertools

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
#
# Things I would change if I had time:
#
# 1. Store finger table in a separate object.  Keeping them separate
# just makes a lot of the usage of the tables more complicated.
#
# 2. Don't store data in a dictionary from key to value, instead,
# store it in a dictionary from key to a tuple of the hashed_key and
# the value.  Because I'm constantly need to check the hashed_key
# value to see whether the node is actually responsible for the data
# or not, I'm having to constantly recompute the hash.  Doing it once
# and storing it would be faster.

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
    self.n_backups = 1
    self.logger = _logger.getChild("Node")
    self.data_lock = readwritelock.RWLock()
    self.finger_lock = readwritelock.RWLock()

  @property
  def distance(self) :
    return self.__metric.distance

  @property
  def hash_key(self) :
    return self.__metric.hash_key

  def get_next(self) :
    return self.fingers[0]

  def set_next(self, value) :
    self.fingers[0] = value

  next = property(get_next, set_next, doc="Successor node")

  @property
  def id(self) :
    return self.__id

  @property
  def name(self) :
    return str(self.__uuid)

  def __str__(self) :
    return "Node(id=%d)" % self.id

  @initialization_check
  def _responsible_for(self, key_hash) :
    if self.id == self.predecessor.id :
      # Only node up
      return True
    return (self.distance(key_hash, self.id)
            < self.distance(key_hash, self.predecessor.id))

  @initialization_check
  def __getitem__(self, key) :
    if not self._responsible_for(self.hash_key(key)) :
      raise Exception("Node %d not responsible for key %d"
                      % (self.id, self.hash_key(key)))
    with self.data_lock.rdlocked() :
      return self.data[key]

  @initialization_check
  def __setitem__(self, key, value) :
    self.logger.debug("Setting key %s to value %s", key, value)
    if not self._responsible_for(self.hash_key(key)) :
      raise Exception("Node %d responsible for key %d"
                      % (self.id, self.hash_key(key)))
    with self.data_lock.wrlocked() :
      old_value_exists = True
      try :
        old_value = self.data[key]
      except KeyError :
        old_value_exists = False
      self.data[key] = value
      try :
        self.logger.debug("Backing up in successors")
        current = self
        following = walk(self.next)
        for node in itertools.islice(walk(self.next), self.n_backups) :
          if self.id == node.id :
            break
          node.store_backup(key, value, current)
          current = node
      except Exception :
        self.logger.info("Problem backing up data.  Rolling back")
        if old_value_exists :
          self.data[key] = old_value
        else :
          del self.data[key]
        raise

  @initialization_check
  def store_backup(self, key, value, predecessor) :
    # predecessor is the preceding node, as determined by the node
    # who's data is being backed up.  If this does not match our
    # actual predecessor, there is a problem with the pointers in the
    # ring.  We'll throw an exception, try to fix it, and try setting
    # the values again.
    with self.data_lock.wrlocked() :
      if predecessor.id != self.predecessor.id :
        raise Exception(
          "Mesh corruption: Storing backup for node %d, but actual predecessor is %d" % (predecessor.id, self.predecessor.id))
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
    return (k for k in self.data.iterkeys()
            if self._responsible_for(self.hash_key(k)))
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
    # Because I'm storing not storing the backup data in a separate
    # dictionary, computing the length is not O(1), but O(n).
    with self.data_lock.rdlocked() :
      return sum(1 for key in self.data
                 if self._responsible_for(self.hash_key(key)))


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

  def ping(self) :
    # Default ping method
    return {"id": str(self.id)}

  def get_fingers(self) :
    return dict(zip(self.finger_steps, self.fingers))

  def repair_fingers(self) :
    self.logger.info("Repairing fingers")
    furthest_known = self
    with self.finger_lock.wrlocked() :
      self.logger.debug("Old fingers: %s", [f.id for f in self.fingers])
      for i, finger in enumerate(self.fingers) :
        try :
          finger.ping()
        except (socket.error, socket.timeout) :
          self.fingers[i] = furthest_known
        else :
          furthest_known = finger
      self.logger.debug("Corrected fingers: %s", [f.id for f in self.fingers])
      # All point somewhere, now I can update to correct them.
      self.update_fingers()
      self.logger.debug("Updated corrected fingers: %s",
                        [f.id for f in self.fingers])

  def repair_predecessor(self) :
    # I'll use the finger lock because the predecessor is essentially
    # another finger.
    self.logger.debug("Repairing predecessor")
    with self.finger_lock.wrlocked() :
      self.logger.debug("Old predecessor", self.predecessor.id)
      try :
        self.predecessor.ping()
      except (socket.error, socket.timeout) :
        # No.  I can't be me... Who should it be...
        furthest_known = self
        idx = len(self.fingers)
        while furthest_known.id == self.id and idx >= 0 :
          idx -= 1
          if self.fingers[idx].id != self.id :
            furthest_known = self.fingers[idx]
        if furthest_known.id == self.id :
          self.logger.warn("Unable to find any other nodes")
          self.predecessor = self
          return

        # Need to actually walk the successors...
        predecessor = furthest_known
        for node in walk(furthest_known) :
          if node.id == self.id :
            break
          else :
            predecessor = node
        self.predecessor = predecessor


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
        if old_finger.id == self.id and not old_finger is self :
          self.logger.warn("Somehow have a finger to a proxy of myself!")
          self.fingers[i] = self
          old_finger = self
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
      self.logger.debug("End updating fingers for new node")


  @initialization_check
  def prepend_node(self, newnode) :
    # By making the method a "prepend node" called on the new
    # successor, I can reduce the traffic by combining the data and
    # fingers for the new node.  More importantly, I can lock the data
    # so any incoming stores for data that would no longer be in the
    # successor's control is blocked.

    # Ensure the node is correct
    with self.finger_lock.wrlocked() :
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
        for k, v in self.data.iteritems() :
          if (self.distance(self.hash_key(k), newnode.id)
              < self.distance(self.hash_key(k), self.id)) :
            delegated_data[k] = v
        self.logger.debug("Sending data: %s", delegated_data)
        newnode.setup(predecessor, dict(predecessor.get_fingers()),
                      delegated_data)

        # Establish new fingers to bring the new node into chain
        self.logger.debug("Setting successor of predecessor to the new node")
        predecessor.next = newnode
        self.logger.debug("Setting my predecessor to new node")
        self.predecessor = newnode

        self.update_fingers_on_insert(newnode)

        for k in delegated_data :
          # Need to check whether I should keep this as a backup...
          del self.data[k]


  def setup(self, predecessor, fingers, data) :
    with self.finger_lock.wrlocked() :
      self.logger.debug("Setting up node with predecessor: %s", predecessor.id)
      self.predecessor = predecessor
      self.logger.debug("Setting up node with initial fingers: %s",
                        [(finger.id, getattr(finger, "url", None))
                         for finger in fingers.values()])
      self.fingers = fingers.values()
    with self.data_lock.wrlocked() :
      self.logger.debug("Setting up node with data: %s", data)
      self.data.update(data)
    self.initialized = True


  def predecessor_leaving(self, new_predecessor, data) :
    with self.data_lock.wrlocked() :
      with self.finger_lock.wrlocked() :
        self.logger.info("Predecessor %d shutting down", self.predecessor.id)
        self.logger.debug("New predecessor %d", new_predecessor.id)
        self.logger.debug("Taking over data: %s", data)
        self.data.update(data)
        self.predecessor = new_predecessor

  def successor_leaving(self, new_successor) :
    with self.finger_lock.wrlocked() :
      old_successor = self.fingers[0]
      for i, finger in enumerate(self.fingers) :
        if finger.id == old_successor.id :
          self.fingers[i] = new_successor

  def leave(self) :
    with self.data_lock.wrlocked() :
      with self.finger_lock.rdlocked() :
        self.logger.info("Disconnecting from peers")
        successor = self.next
        if successor.id != self.id :
          self.logger.debug("Sending data: %s", self.data)
          successor.predecessor_leaving(self.predecessor, self.data)
        # This will throw...  Need to add a setter.
        if self.predecessor.id != self.id :
          self.predecessor.successor_leaving(successor)

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
