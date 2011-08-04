# Heavily based on example at <http://www.linuxjournal.com/article/6797>
import hashlib
import uuid
import bisect
from collections import MutableMapping


class Md5Metric(object) :
  # Since both the md5 and uuid are 128-bit, I can use the former for
  # hashing, and the latter for the node ids and the sizes already work.

  def __init__(self, hash_bits=128) :
    self.hash_bits = 128

  # I don't want to shadow a builtin, so I'll give it this clumsy name.
  def hash_key(self, key) :
    """Hash function for finding the appropriate node"""
    return int(hashlib.md5(key).hexdigest(), 16) % 2**self.hash_bits


  # Clockwise ring function taken from
  # <http://www.linuxjournal.com/article/6797>
  def distance(self, a, b) :
    # k is the number of bits in the ids used.  For a uuid, this is 128
    # bytes.  If this were C, one would need to worry about overflow,
    # because 2**128 takes 129 bytes to store.
    if a < b:
      return b-a
    elif a == b :
      return 0
    else :
      return (2**self.hash_bits) + (b-a)

class TrivialMetric(Md5Metric) :
  def __init__(self, hash_bits) :
    Md5Metric.__init__(self, hash_bits)

  def hash_key(self, key) :
    return key % 2**self.hash_bits



# Keep the finger_table_size small for testing, so I can get my head around it
finger_table_size = 128

# Surprisingly, chopping to integers after the exponentiation returns
# better step sizes, although they are no longer powers of two.  (Need
# to be careful when finger_table_size > hash_bits, there are
# duplicate step sizes of 0.)
def computer_finger_steps(hash_bits, finger_table_size) :
  return sorted(set(int(2**((hash_bits-1)*x*1.0/(finger_table_size)))
                    for x in xrange(finger_table_size+1)))[:-1]




# Node finding function taken from <http://www.linuxjournal.com/article/6797>
def find_predecessor(start, key_hash) :
  current = start
  while True :
    current_distance = current.distance(current.id, key_hash)
    # Can speed up by not checking all fingers, and use the finger
    # step size to narrow down which finger it is.
    if current_distance == 0 :
      return current
    idx = bisect.bisect_right(current.finger_steps, current_distance)
    idx -= 1
    if idx < 0 :
      return current
    finger = current.fingers[idx]
    while idx >= 0 :
      # print "idx =", idx
      while finger is None :
        # Possible node corruption
        idx -= 1
        if idx < 0 :
          # We've gone through all the fingers before where we should
          # go, and they're not pointing to anything.  Possibly can
          # recover if some of the longer fingers are not None, but
          # won't worry about that now.
          raise Exception("Node graph corruption: Dangling end")
        finger = current.fingers[idx]
      if current_distance > current.distance(finger.id, key_hash) :
        current = finger
        break
      else :
          idx -= 1
          finger = current.fingers[idx]
    else :
      return current
  return current


# Non-finger-based lookup logic, so I can replace the new logic with
# the old for timing purposes.
def find_predecessor_without_fingers(start, key_hash) :
  current = start
  while True :
    if (current.distance(current.id, key_hash)
        > current.distance(current.next.id, key_hash)) :
      current = current.next
    else :
      break
  return current

def find_node(start, key_hash) :
  rslt = find_predecessor(start, key_hash)
  return rslt.next


# I could probably derive from a dictionary, and just add extra
# properties and methods, but I might need to change too many
# functions, especially when I want to persist the data to disk.
class Node(MutableMapping) :
  def __init__(self, id=None, nfingers=None, metric=None) :
    # nfingers   None means default

    # uuid4 is not uniform over 2**128 because hex digit 13 is always
    # 4, and hex digit 17 is either 8, 9, A, or B.  But since these
    # are low digits, it shouldn't matter unless the number of nodes
    # becomes super-large (~2**32 or so)
    if id is None :
      self.__uuid = uuid.uuid4()
    else :
      self.__uuid = uuid.UUID(int=id)
    self.__metric = metric if metric else Md5Metric()
    self.__id = self.__uuid.int % 2**self.__metric.hash_bits
    self.data = {}
    self.predecessor = None
    self.finger_steps = computer_finger_steps(
      self.__metric.hash_bits, finger_table_size)
    self.fingers = [None for f in self.finger_steps]

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
   
  def __getitem__(self, key) :
    return self.data[key]

  def __setitem__(self, key, value) :
    self.data[key] = value

  def __delitem__(self, key) :
    del self.data[key]

  def iterkeys(self) :
    return self.data.iterkeys()
  __iter__ = iterkeys

  def __contains__(self, key) :
    return key in self.data

  def __len__(self) :
    return len(self.data)

  # def find_successor(self, key_hash) :
  #   if distance(self.id, key_hash) >= distance(self.predecessor.id, key_hash) :
  #     return self
  #   else :
  #     return self.closest_preceding_node(key_hash).find_successor(key_hash)

  # def closest_preceding_node(self, key_hash) :
  #   for finger in reversed(self.fingers) :
  #     if distance(finger.id, key_hash) > distance(self.id, key_hash) :
  #       return finger
  #   return self

  def update_fingers(self) :
    for i, step in enumerate(self.finger_steps) :
      old = self.fingers[i]
      self.fingers[i] = find_node(old, ((self.id+step-1)
                                        % 2**self.__metric.hash_bits))

  def update_fingers_on_insert(self, newnode) :
    # Faster updating when new node is added.
    for i, step in enumerate(self.finger_steps) :
      if (self.distance(self.id, self.fingers[i].id)
          < self.distance(self.id, newnode.id)) :
        continue
      old = self.fingers[i]
      self.fingers[i] = find_node(old, ((self.id+step-1)
                                        % 2**self.__metric.hash_bits))
      if self.fingers[i].id == old.id :
        break


def iternodes(start) :
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


class DistributedHash(object) :
  def __init__(self, start=None) :
    # Start points to the beginning of the list
    self.__start = start

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
    return iternodes(self.__start)

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
      return

    predecessor = find_predecessor(self.__start, newnode.id)
    if predecessor.id == newnode.id :
      raise Exception("Node already exists with same id")
    # Start with all fingers pointing to the predecessor, then update
    newnode.predecessor = predecessor
    newnode.fingers = list(predecessor.fingers)
    newnode.update_fingers()

    successor = predecessor.next
    for k, v in successor.iteritems() :
      if (newnode.distance(newnode.id, newnode.hash_key(k)) >
          newnode.distance(successor.id, newnode.hash_key(k))) :
        newnode[k] = v
    predecessor.fingers[0] = newnode
    successor.predecessor = newnode

    # Update fingers of other nodes.
    #
    # (a) only nodes from new_node._id - max(finger_step) to predecessor
    # can possible have changes
    #
    # (b) for each node, only fingers that are from 1 to (new_node._id
    # - node._id) need to change.
    for node in iternodes(find_predecessor(
      newnode, newnode.id - max(newnode.finger_steps))) :
      if node.id == newnode.id :
        break
      node.update_fingers_on_insert(newnode)

    for k in newnode :
      del successor[k]


  def leave(self, node) :
    if self.num_nodes() == 0 :
      return

    # Note I am looking up, because then I can get a node to leave by
    # passing in another instance with the same id.  Might be useful
    # for testing.
    predecessor = find_predecessor(self.__start, node.id).predecessor
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
