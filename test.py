#!/usr/bin/env python

import unittest

import dyschord




# DistributedHash
#
# Simple client for a collection of Nodes, all in the same process.
# This was designed to quickly test the algorithms used in Node in the
# unittests.
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
    node = dyschord.find_node(self.__start, key_hash)
    return node.data[key]

  def store(self, key, value) :
    key_hash = self.__start.hash_key(key)
    node = dyschord.find_node(self.__start, key_hash)
    node[key] = value


  def delete(self, key) :
    key_hash = self.__start.hash_key(key)
    node = dyschord.find_node(self.__start, key_hash)
    del node[key]

  def _iternodes(self, start=None) :
    if self.__start is None :
      return []
    return dyschord.walk(self.__start)

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

    predecessor = dyschord.find_predecessor(self.__start, newnode.id)
    successor = predecessor.next
    if successor.id == newnode.id :
      raise Exception("Node already exists with same id")
    successor.prepend_node(newnode)

    # Optimize fingers.  Don't need to lock the nodes while this is
    # being done.
    newnode.update_fingers()
    dyschord.announce(newnode)


  def leave(self, node) :
    if self.num_nodes() == 0 :
      return

    # Note I am looking up, because then I can get a node to leave by
    # passing in another instance with the same id.  Might be useful
    # for testing.
    predecessor = dyschord.find_predecessor(self.__start, node.id)
    # print "Leaving node %d has predecessor %d" % (node.id, predecessor.id)
    if predecessor.next.id != node.id :
      # No joined node with this id.  Maybe log the missing node, but
      # work is done
      return
    
    leaving = predecessor.next
    # Check we aren't removing first item
    if leaving.id == self.__start.id :
      self.__start = leaving.next
    leaving.leave()


# Simple DistributedHash builder for testing
def construct_dh(size, Node=dyschord.Node) :
  nodes = {}
  while len(nodes) != size :
    new_node = Node()
    nodes[new_node.id] = new_node
  rslt = DistributedHash()
  for n in nodes.itervalues() :
    rslt.join(n)
  return rslt

# Again a function for testing, we'll build a list using the first n words from
# the system dictionary
def fill_with_words(dh, n) :
  i = 0
  for word in open("/etc/dictionaries-common/words") :
    word = word.strip()
    if not word :
      continue
    dh.store(word.strip(), i)
    i += 1
    if i == n:
      break
  return i

# Not needed any more, right now, but will probably be needed soon.

# # Really this can be used for anything, but it's such a naive
# # patching, for anything more complicated, I'd rather use a library
# # developed by someone else.
# def patch_dyschord(settings=dict(), module=dyschord) :
#   orig_settings = {}
#   for k, v in settings.iteritems() :
#     orig_settings[k] = getattr(module, k)
#     setattr(module, k, v)
#   return orig_settings



def dump_distributed_hash(dh) :
  for node in dh._iternodes() :
    print "id=%s, next.id=%s, len=%s" % (node.id, node.next.id, len(node))


class InitializationLockTest(unittest.TestCase) :
  def setUp(self) :
    self.metric = dyschord.TrivialMetric(4)
    self.Node = (lambda id=None :
                   dyschord.Node(id, nfingers=1, metric=self.metric))
    self.nodes = dict((i, self.Node(i)) for i in (0, 3, 8))

  def testUninitialized(self) :
    node = self.nodes.values()[0]
    # print "Initialization state:", node.initialized
    # print len(node)
    # print node.closest_preceding_node(10)
    self.assertRaises(dyschord.Uninitialized, len, node)

  def testInitialized(self) :
    node = self.nodes.values()[0]
    dh = DistributedHash(node)
    self.assertEquals(len(node), 0)


class JoinTest(unittest.TestCase) :
  def setUp(self) :
    self.metric = dyschord.TrivialMetric(4)
    self.Node = (lambda id=None :
                   dyschord.Node(id, nfingers=1, metric=self.metric))
    self.nodes = dict((i, self.Node(i)) for i in (0, 3, 8))

  def testJoinEmpty(self) :
    dh = DistributedHash()
    self.assertEquals(dh.num_nodes(), 0)
    for i, node in enumerate(self.nodes.itervalues()) :
      dh.join(node)
      self.assertEquals(dh.num_nodes(), i+1)

  def testJoinDuplicateId(self) :
    dh = DistributedHash()
    for i, node in enumerate(self.nodes.itervalues()) :
      dh.join(node)
    new_node = self.Node(id=self.nodes.values()[0].id)
    self.assertRaises(Exception, dh.join, new_node)

  def testJoinWithData(self) :
    dh = DistributedHash()
    for node in self.nodes.itervalues() :
      dh.join(node)
    dh.store("1", "one")
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=3 else 1)
    self.nodes[5] = self.Node(5)
    dh.join(self.nodes[5])
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=3 else 1)

  def testJoinWithDataMove(self) :
    dh = DistributedHash()
    for node in self.nodes.itervalues() :
      dh.join(node)
    dh.store("1", "one")
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=3 else 1)
    self.nodes[2] = self.Node(2)
    dh.join(self.nodes[2])
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=2 else 1)

  def testJoinWithDataMoveMatchNewId(self) :
    dh = DistributedHash()
    for node in self.nodes.itervalues() :
      dh.join(node)
    dh.store("1", "one")
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=3 else 1)
    self.nodes[1] = self.Node(1)
    dh.join(self.nodes[1])
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=1 else 1)


class LeaveTest(unittest.TestCase) :
  def setUp(self) :
    self.metric = dyschord.TrivialMetric(4)
    self.Node = lambda i=None : dyschord.Node(i, nfingers=1, metric=self.metric)
    self.nodes = dict((i, self.Node(i)) for i in (0, 3, 8))
    self.distributed_hash = DistributedHash()
    for node in self.nodes.itervalues() :
      self.distributed_hash.join(node)

  def testLeaveEmpty(self) :
    dh = self.distributed_hash
    self.assertEquals(dh.num_nodes(), len(self.nodes))
    dh.leave(self.nodes[0])
    self.assertEquals(dh.num_nodes(), len(self.nodes)-1)

  def testLeaveWithData(self) :
    dh = self.distributed_hash
    dh.store("1", "one")
    self.assertEqual(len(dh), 1)
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=3 else 1)
    dh.leave(self.nodes[8])
    self.assertEquals(len(dh), 1)
    for k, node in self.nodes.iteritems() :
      if k == 8 :
        continue
      self.assertEquals(len(node), 0 if k!=3 else 1)

  def testLeaveWithDataMove(self) :
    dh = self.distributed_hash
    dh.store("1", "one")
    self.assertEquals(len(dh), 1)
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=3 else 1)
    dh.leave(self.nodes[3])
    self.assertEquals(len(dh), 1)
    for k, node in self.nodes.iteritems() :
      if k == 3 :
        continue
      self.assertEquals(len(node), 0 if k!=8 else 1)

  def testLeaveWithDataMoveMatchOldId(self) :
    dh = self.distributed_hash
    dh.store("0", "zero")
    self.assertEquals(len(dh), 1)
    for k, node in self.nodes.iteritems() :
      self.assertEquals(len(node), 0 if k!=0 else 1)
    dh.leave(self.nodes[0])
    self.assertEquals(len(dh), 1)
    for k, node in self.nodes.iteritems() :
      if k == 0 :
        continue
      self.assertEquals(len(node), 0 if k!=3 else 1)


class SimpleTest(unittest.TestCase) :
  def setUp(self) :
    self.metric = dyschord.TrivialMetric(4)
    self.Node = lambda i=None : dyschord.Node(i, nfingers=1, metric=self.metric)
    self.distributed_hash = construct_dh(5, self.Node)

  def testOneValue(self) :
    self.assertEquals(len(self.distributed_hash), 0)
    self.distributed_hash.store("0", "zero")
    self.assertEquals(len(self.distributed_hash), 1)
    self.assertEquals(self.distributed_hash.lookup("0"), "zero")

  def testLookupMissing(self) :
    self.assertRaises(KeyError, self.distributed_hash.lookup, 0)

  def testDeletion(self) :
    self.assertEquals(len(self.distributed_hash), 0)
    self.distributed_hash.store("0", "zero")
    self.assertEquals(len(self.distributed_hash), 1)
    self.distributed_hash.delete("0")
    self.assertEquals(len(self.distributed_hash), 0)
    self.assertRaises(KeyError, self.distributed_hash.lookup, 0)

  def testJoinPreExisting(self) :
    distributed_hash = DistributedHash()
    n = self.Node(1)
    distributed_hash.join(n)
    self.assertRaises(Exception, distributed_hash.join, n)
    

class WordsTest(unittest.TestCase) :
  def setUp(self) :
    self.distributed_hash = construct_dh(10)


  def testJoin(self) :
    size = 10000
    fill_with_words(self.distributed_hash, size)
    self.assertEquals(len(self.distributed_hash), size)
    old_num_nodes = self.distributed_hash.num_nodes()
    old_len = len(self.distributed_hash)
    as_dict = dict((k, self.distributed_hash.lookup(k))
                   for k in self.distributed_hash.iterkeys())
    existing_nodes = set(n.id for n in self.distributed_hash._iternodes())
    while True :
      new_node = dyschord.Node()
      if new_node.id not in existing_nodes :
        break
    self.distributed_hash.join(new_node)
    self.assertEquals(self.distributed_hash.num_nodes(), old_num_nodes+1)
    self.assertEquals(len(self.distributed_hash), old_len)
    for k, v in as_dict.iteritems() :
      self.assertEquals(self.distributed_hash.lookup(k), v)

  # Need a test for adding a node with id just below the current
  # lowest, to make sure that the code properly partitions all the
  # data.
      
  def testLeaveNoncontained(self) :
    self.assertEquals(DistributedHash().leave(dyschord.Node()), None)

  def testLeaveNonfirst(self) :
    size = 10
    fill_with_words(self.distributed_hash, size)
    old_num_nodes = self.distributed_hash.num_nodes()
    nodes = list(self.distributed_hash._iternodes())
    as_dict = dict((k, self.distributed_hash.lookup(k))
                   for k in self.distributed_hash.iterkeys())
    nodes = list(self.distributed_hash._iternodes())
    parting = nodes[(1+old_num_nodes)/2]
    self.distributed_hash.leave(parting)
    self.assertEqual(len(self.distributed_hash), size)
    self.assertEquals(self.distributed_hash.num_nodes(), old_num_nodes-1)
    for k, v in as_dict.iteritems() :
      self.assertEquals(self.distributed_hash.lookup(k), v)

  def testLeaveFirst(self) :
    fill_with_words(self.distributed_hash, 10000)
    old_num_nodes = self.distributed_hash.num_nodes()
    as_dict = dict((k, self.distributed_hash.lookup(k))
                   for k in self.distributed_hash.iterkeys())
    parting = list(self.distributed_hash._iternodes())[0]
    self.distributed_hash.leave(parting)
    self.assertEqual(len(self.distributed_hash), 10000)
    self.assertEquals(self.distributed_hash.num_nodes(), old_num_nodes-1)
    for k, v in as_dict.iteritems() :
      self.assertEquals(self.distributed_hash.lookup(k), v)


if __name__=="__main__" :
  unittest.main()
