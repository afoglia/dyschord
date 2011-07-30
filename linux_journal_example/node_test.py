#!/usr/bin/env python

import unittest

import node


# Simple list builder for testing
def construct_list(size) :
  nodes = [node.Node() for i in xrange(size)]
  nodes.sort(key=lambda n: n.id)
  start = nodes[0]
  prev = start
  for n in nodes[1:] :
    prev.next = n
    prev = n
  n.next = start
  return nodes

# Again a function for testing, we'll build a list using the first n words from
# the system dictionary
def fill_with_words(dh, n) :
  for i, word in enumerate(open("/etc/dictionaries-common/words")) :
    if i==n :
      break
    dh.store(word.strip(), i)
  return i


class SimpleTest(unittest.TestCase) :
  def setUp(self) :
    nodes = construct_list(10)
    self.distributed_hash = node.DistributedHash(nodes[0])

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

  def testJoin(self) :
    fill_with_words(self.distributed_hash, 10000)
    self.assertEquals(len(self.distributed_hash), 10000)
    old_num_nodes = self.distributed_hash.num_nodes()
    as_dict = dict((k, self.distributed_hash.lookup(k))
                   for k in self.distributed_hash.iterkeys())
    new_node = node.Node()
    self.distributed_hash.join(new_node)
    self.assertEquals(len(self.distributed_hash), 10000)
    self.assertEquals(self.distributed_hash.num_nodes(), old_num_nodes+1)
    for k, v in as_dict.iteritems() :
      self.assertEquals(self.distributed_hash.lookup(k), v)

if __name__=="__main__" :
  unittest.main()
