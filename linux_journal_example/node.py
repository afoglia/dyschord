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


class Node(object) :
  def __init__(self) :
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



# Node finding function taken from <http://www.linuxjournal.com/article/6797>
def find_node(start, key_hash) :
  current = start
  if current.next is None :
    raise Exception("Node graph corruption: Dangling end")
  while (distance(current.id, key_hash) >
         distance(current.next.id, key_hash)) :
    current = current.next
  return current


def lookup(start, key) :
  # Can't just use hash because that might differ between Python
  # implementations.  Plus by using a different hash function, we
  # reduce the changes of collisions in the dictionaries in each node
  # assuming the algorithms are sufficiently different.  And Python's
  # hashing algorithm is relatively simple, so that's probably the
  # case.
  key_hash = hash_key(key)
  node = find_node(start, key_hash)
  return node.data[key]


def store(start, key, value) :
  key_hash = hash_key(key)
  node = find_node(start, key_hash)
  node[key] = value


def delete(start, key) :
  key_hash = hash_key(key)
  node = find_node(start, key_hash)
  del node[key]

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
