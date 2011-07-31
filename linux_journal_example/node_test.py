#!/usr/bin/env python

import unittest

import node


# Simple DistributedHash builder for testing
def construct_dh(size) :
  nodes = {}
  while len(nodes) != size :
    new_node = node.Node()
    nodes[new_node.id] = new_node
  rslt = node.DistributedHash()
  for n in nodes.itervalues() :
    rslt.join(n)
  return node.DistributedHash(n)

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


class SimpleTest(unittest.TestCase) :
  def setUp(self) :
    self.distributed_hash = construct_dh(100)

  def testOneValue(self) :
    self.assertEquals(len(self.distributed_hash), 0)
    self.distributed_hash.store("foo", 0)
    self.assertEquals(len(self.distributed_hash), 1)    
    self.assertEquals(self.distributed_hash.lookup("foo"), 0)

  def testLookupMissing(self) :
    self.assertRaises(KeyError, self.distributed_hash.lookup, "spam")

  def testDeletion(self) :
    self.assertEquals(len(self.distributed_hash), 0)
    self.distributed_hash.store("foo", 0)
    self.assertEquals(len(self.distributed_hash), 1)
    self.distributed_hash.delete("foo")
    self.assertEquals(len(self.distributed_hash), 0)
    self.assertRaises(KeyError, self.distributed_hash.lookup, "foo")

  def testJoinPreExisting(self) :
    distributed_hash = node.DistributedHash()
    n = node.Node(1)
    distributed_hash.join(n)
    self.assertRaises(Exception, distributed_hash.join, n)
    
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
      new_node = node.Node()
      if new_node.id not in existing_nodes :
        break
    self.distributed_hash.join(new_node)
    self.assertEquals(len(self.distributed_hash), size)
    self.assertEquals(self.distributed_hash.num_nodes(), old_num_nodes+1)
    self.assertEquals(len(self.distributed_hash), old_len)
    for k, v in as_dict.iteritems() :
      self.assertEquals(self.distributed_hash.lookup(k), v)

  # Need a test for adding a node with id just below the current
  # lowest, to make sure that the code properly partitions all the
  # data.
      
  def testLeaveNoncontained(self) :
    self.assertEquals(node.DistributedHash().leave(node.Node()), None)

  def testLeaveNonfirst(self) :
    size = 10
    fill_with_words(self.distributed_hash, size)
    old_num_nodes = self.distributed_hash.num_nodes()
    as_dict = dict((k, self.distributed_hash.lookup(k))
                   for k in self.distributed_hash.iterkeys())
    parting = list(self.distributed_hash._iternodes())[(1+old_num_nodes)/2]
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
