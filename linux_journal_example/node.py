# Heavily based on example at <http://www.linuxjournal.com/article/6797>
import hashlib
import uuid
from collections import MutableMapping



# I don't want to shadow a builtin, so I'll give it this clumsy name.
def hash_key(key) :
  """Hash function for finding the appropriate node"""
  return int(hashlib.md5(key).hexdigest(), 16) % 2**hash_bits
# Since both the md5 and uuid are 128-bit, I can use the former for
# hashing, and the latter for the node ids and the sizes already work.
hash_bits = 128

# Keep the finger_table_size small for testing, so I can get my head around it
finger_table_size = 128

# Surprisingly, chopping to integers after the exponentiation returns
# better step sizes, although they are no longer powers of two.  (Need
# to be careful when finger_table_size > hash_bits, there are
# duplicate step sizes of 0.)
finger_steps = sorted(set(int(2**((hash_bits-1)*x*1.0/(finger_table_size)))
                          for x in xrange(finger_table_size+1)))[:-1]



# Clockwise ring function taken from <http://www.linuxjournal.com/article/6797>
def distance(a, b) :
  # k is the number of bits in the ids used.  For a uuid, this is 128
  # bytes.  If this were C, one would need to worry about overflow,
  # because 2**128 takes 129 bytes to store.
  if a < b:
    return b-a
  elif a == b :
    return 0
  else :
    return (2**hash_bits) + (b-a)


# Node finding function taken from <http://www.linuxjournal.com/article/6797>
def find_predecessor(start, key_hash) :
  current = start
  all_fingers_none = True
  while True :
    for finger in reversed(current.fingers) :
      if finger is None :
        continue
      all_fingers_none = False
      if distance(current.id, key_hash) > distance(finger.id, key_hash) :
        current = finger
        break
    else :
      return current
  if all_fingers_none :
      raise Exception("Node graph corruption: Dangling end")
  return current

# Non-finger-based lookup logic, so I can replace the new logic with
# the old for timing purposes.
def find_predecessor_without_fingers(start, key_hash) :
  current = start
  while True :
    if distance(current.id, key_hash) > distance(current.next.id, key_hash) :
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
  def __init__(self, id=None) :
    # uuid4 is not uniform over 2**128 because hex digit 13 is always
    # 4, and hex digit 17 is either 8, 9, A, or B.  But since these
    # are low digits, it shouldn't matter unless the number of nodes
    # becomes super-large (~2**32 or so)
    if id is None :
      self.__uuid = uuid.uuid4()
    else :
      self.__uuid = uuid.UUID(int=id)
    self.__id = self.__uuid.int % 2**hash_bits
    self.data = {}
    self.predecessor = None
    self.fingers = [None for f in finger_steps]
    self.finger_steps = finger_steps

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

  def update_fingers(self) :
    for i, step in enumerate(self.finger_steps) :
      old = self.fingers[i]
      self.fingers[i] = find_node(old, ((self.id+step-1) % 2**hash_bits))

  def update_fingers_on_insert(self, newnode) :
    # Faster updating when new node is added.
    for i, step in enumerate(self.finger_steps) :
      if distance(self.id, self.fingers[i].id) < distance(self.id, newnode.id) :
        continue
      old = self.fingers[i]
      self.fingers[i] = find_node(old, ((self.id+step-1) % 2**hash_bits))
      if self.fingers[i].id == old.id :
        break


class DistributedHash(object) :
  def __init__(self, start=None) :
    # Start points to the beginning of the list
    self.__start = start

  # Node finding function taken from <http://www.linuxjournal.com/article/6797>
  def _find_node(self, key_hash) :
    return find_node(self.__start, key_hash)

  def lookup(self, key) :
    # Can't just use hash because that might differ between Python
    # implementations.  Plus by using a different hash function, we
    # reduce the changes of collisions in the dictionaries in each node
    # assuming the algorithms are sufficiently different.  And Python's
    # hashing algorithm is relatively simple, so that's probably the
    # case.
    key_hash = hash_key(key)
    node = self._find_node(key_hash)
    return node.data[key]

  def store(self, key, value) :
    key_hash = hash_key(key)
    node = self._find_node(key_hash)
    node[key] = value


  def delete(self, key) :
    key_hash = hash_key(key)
    node = self._find_node(key_hash)
    del node[key]

  def _iternodes(self, start=None) :
    node = start if start is not None else self.__start
    seen = set()
    if node is not None :
      while True :
        if node.id in seen :
          raise Exception("Infinite loop.  Seen %s twice" % node.id)
        seen.add(node.id)
        yield node
        node = node.next
        if node.id == self.__start.id :
          break

  def iterkeys(self) :
    for node in self._iternodes() :
      for k in node.iterkeys() :
        yield k

  def __len__(self) :
    return sum(len(node) for node in self._iternodes())

  def clear(self) :
    for node in _iternodes(self) :
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
      if (distance(newnode.id, hash_key(k)) >
          distance(successor.id, hash_key(k))) :
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
    for node in self._iternodes(
      find_predecessor(newnode, newnode.id - max(newnode.finger_steps))) :
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
