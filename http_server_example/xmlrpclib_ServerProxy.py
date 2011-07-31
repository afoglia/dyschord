#!/usr/bin/env python

# Example of xmlrpclib library from The Python Standard Library by
# Example, Sections 12.10 and 12.11

import xmlrpclib  

server = xmlrpclib.ServerProxy("http://localhost:9000")
print "Ping:", server.ping()
