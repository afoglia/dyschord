# Distributed Key-Value Storage Example
Anthony Foglia

This in an example of a distributed key-value store based on the
[Chord algorithm](http://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf).
It was done as a coding exercise for an employer I was applying to.  I
have tried to remove the employer's name in case they continue to use
the question.  As a coding exercise, I have not tested this on
different versions of python of done any performance tests.

## Requirements

* Python 2.7

## Installation

dyschord comes with a distribute build script, so installation is simply

    $ python setup.py

There is also a simple test script (that tests the Chord algorithms,
but nothing over the network), but I have not configured the setup.py
to use it.

## Running the client

Client code interacts with the service via the dyschord.Client object.
Creating an instance takes two parameters:

* peers: A list of urls of the peers to attempt to connect to, at
  least one of which must be up
* min_connections: The mininum number of connections to try to
  maintain with the server.  (optional, default 3)

The client does not keep any connections to the servers open, so it
will not be notified as peers go down.  Provided at least one
connection is up, on each lookup or storage request, it will try to
maintain at least the minimum number of connections.

The Client object has two main methods:

* lookup(key)
* store(key, value)

Keys must be strings.  (Note, to avoid even the possibility of unicode
problems, they need to be Python 2.7 strings, not unicode.)  Values
can be any object that can be json encoded.

## Running the server

The server can be run from the command `dyschord-server`.  The default
configuration is read from a json file in the default location of
dyschord.conf.  The configuration is a serialized dictionary.  The
most common configuration parameters are:

* cloud_members: A list of other nodes to connect to attempt to
  connect to on startup.
* heartbeat: How often in seconds to check the stability of the mesh.
* node_id:  The id number to use for the node
* port: The port number to listen on

The last two can also be set from the command-line with the options
`--id` and `--port` respectively.

Additional parameters include

* metric: One of "md5" (default) or "trivial".  The former uses an md5
  hash of the keys to map them to a 128-bit space of nodes.  The
  latter maps keys to hashes by converting the keys to integers and
  using their value mod 16.  It's much smaller (and only allows 16
  nodes), and requires all keys to be integers, but makes debugging
  easier.
* log_requests: Turn on the logRequests option of the XML-RPC server
* proxy_verbose: Turn on the verbosity for the XML-RPC server proxies

## Architecture

As a Chord implementation, each server process is a member of a mesh
or cloud of nodes, responsible for a subset of the hash value space.
As requested, the client talks to the server(s) over HTTP, and the
server nodes use the same protocol to talk amongst themselves.  The
protocol is built atop XML-RPC, not because of its technical
superiority, but because the standard library modules handle most of
the network transferring (e.g. headers, serializing most objects) and
easiest for me to quickly learn.  I could have went with a JSON-RPC
package, which would use less bandwidth, but there was much less
documentation than the standard library packages.

One drawback is that the standard library XML-RPC server is not
multi-threaded.  But I found a recipe on StackOverflow to add
threading support (so each request runs in its own thread) that
required only small changes to get working on Python 2.7.

The system has three main classes, `Node`, `NodeProxy`, `DyschordService`.

### Node

The Node class is the main class where all the Chord algorithm logic
resides.  Each node maintains a pointer to its predecessor and a
finger table to successive nodes.  Also, should the process crash, the
data for each node is backed up in the successor node.

### NodeProxy

Since serializing entire nodes would be unacceptable, any remote nodes
that need to be interacted with are done via a NodeProxy object.  It
is created from a dictionary of the necessary node identication data
(the node id and its url).  It's responsible for serializing and node
information sent to remote nodes, and deserializing any node
responses.

### DyschordService

This class wraps a Node and is registered with the XML-RPC server.  It
handles the other side of the translation that the NodeProxy does.
Also, as a simple check, in case there is a problem looking up or
storing data caused by a missing node, it will try to repair the
predecessor and finger tables and then repeat the request.  Its
another level of protection, should a node go down during a query.

### Additional classes

The other main classes are 

#### PredecessorMaintainer

A Thread class that regularly checks that if predecessor node has gone
down, and if so, tries to repair the mesh.

#### Metric

A class to hold the information about the hashing and distance
functions used on the chord.  Developed so I could use a simpler,
trivial metric for testing.  Do not mix nodes using different metrics.
There is no check against this.

#### NodeTranslation

A simple class to store the NodeTranslation rules in a common place
for the NodeProxy and DyschordService to both use.  More importantly,
it ensures that local nodes are never referred through via proxies (to
avoid possibly interacting with the same node in the same flow of
commands but from different threads and getting deadlock issues).

## Algorithmic notes

The algorithm is the basic Chord algorithm, with some optimizations of
the updating of finger tables on node arrival and departure.  The
latter boil down to only checking fingers that might have changed, so
instead of O(f log(n)) where f is the number of items in the finger
table, it's closer to O(log(n)).  (The exact amount depends on the
density of nodes.)

## Issues

The standard library json decoder only produces unicode strings, so
returned strings (whether the entire value or part) are unicode.

As stated above, the metric used is a parameter of the node, but there
is no checks to ensure a cloud rejects members with a different
metric.  As there is no reason to use anything other than md5 in
production, this check wasn't high priority.

## Changes i would make but didn't have time:

1. *Persistence:* Nowhere do I actually save any of the data to disk.
I started work on persisting the data to a hashdb instance, but it's
not complete.

2. *Minor refactorings:* The NodeTranslation class was a very last
minute addition, as a result, many of the old functions it replaces
are still there, albeit they call the proper NodeTranslation methods.
But it's a level of indirection I could easily remove if I had time.
Also, I would try changing the finger table storage from two separate
lists to one object.  Then a lot of the traversing logic can be
combined and hopefully the code made clearer.

3. *Running as daemon:* The server should modified so it can run as a
daemon.  While this would make it easier to install, this doesn't add
to the threading or networking complexity though.  In the meantime,
one can run it under screen or with nohup on for the same effect.

4. *Complex refactorings:* I started by storing the backup data for a
node in the same dictionary as the successor node uses for the data it
owns (i.e. all in a data property of the Node), thinking it would
allow quicker recovery from node failure.  And in some sense it does,
but storing it in a separate dictionary (say Node.backup_data) might
actually make the logic for node joins and disconnects easier.  I
wouldn't know till I code it.

5. *Other DHT algorithms:* I would like to try the
[the Kademlia](http://www.cs.rice.edu/Conferences/IPTPS02/109.pdf)
and [Koorde](http://iptps03.cs.berkeley.edu/final-papers/koorde.ps)
algorithms.  The former uses a symmetric distance so keeping the
finger table correct is supposedly easier.  The latter gives better
performance for lookups (O(log(n)/log(log(n)) vs. O(log(n) for Chord
with finger tables)

## Possible problems

I did not make the Client itself multi-threaded.  The only thing that
would need to be mulitthreaded is the method that updates the
collection of known peers.  But I focused more on handling the
threading on the server side.

There is no security, either authentication or encryption, of the
network transfers.  The problem statement claims that this service
would run on the backend servers, which would be protected from random
malicious users.  This is not ideal--if a hacker gets into the local
network he could do a lot of damage to the service--but I think fair
within the parameters of the assignment.  Any security ideas I have
would require using HttpsConnections, which would involve security
certificates.  (A simple plaintext password would help accidental
interference, but have no effect against a malicious user.)
