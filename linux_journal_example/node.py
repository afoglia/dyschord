# Heavily based on example at <http://www.linuxjournal.com/article/6797>
import hashlib
import uuid



# This needs a different name.  I don't want to shadow a builtin.
def hash_key(key) :
  """Hash function for finding the appropriate node"""
  return int(hashlib.md5(key).hexdigest(), 16)
# Since both the md5 and uuid are 128-bit, I can use the former for
# hashing, and the latter for the node ids and the sizes already work.
hash_bits = 128

# I could probably derive from a dictionary, or a MutableMapping base
# class, but I might need to change too many functions...
class Node(object) :
  def __init__(self) :
    # uuid4 is not uniform over 2**128 because hex digit 13 is always
    # 4, and hex digit 17 is either 8, 9, A, or B.  But since these
    # are low digits, it shouldn't matter unless the number of nodes
    # becomes super-large (~2**32 or so)
    self.__uuid = uuid.uuid4()
    self.data = {}
    self.next = None

  @property
  def id(self) :
    return self.__uuid.int

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

  def __len__(self) :
    return len(self.data)

  def clear(self) :
    self.data.clear()



class DistributedHash(object) :
  def __init__(self, start=None) :
    # Start points to the beginning of the list
    self.__start = start

  # Node finding function taken from <http://www.linuxjournal.com/article/6797>
  def _find_node(self, key_hash) :
    current = self.__start
    if current.next is None :
      raise Exception("Node graph corruption: Dangling end")
    while (distance(current.id, key_hash) >
           distance(current.next.id, key_hash)) :
      current = current.next
    return current

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

  def _iternodes(self) :
    node = self.__start
    if node is not None :
      while True :
        yield node
        node = node.next
        if node == self.__start :
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

# def join(start, node) :
#   predecessor = find_node(start, node.id)
#   successor = predecessor.next
#   node.next = successor



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
