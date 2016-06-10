#!/usr/bin/env python

import _pytimeseries


ts = _pytimeseries.Timeseries()
print ts
print

print "Asking for ASCII backend by ID:"
# try getting a backend that exists
be = ts.get_backend_by_id(1)
print "Got backend: %d, %s (%s)" % (be.id, be.name, be.enabled)
print

# try to get one that does not exist
print "Asking for non-existent backend by ID (1000):"
be = ts.get_backend_by_id(1000)
print "This should be none: %s" % be
print

# try to get all available backends
print "Getting all available backends:"
all_bes = ts.get_all_backends()
print all_bes
print

# try to get ascii by name
print "Asking for ASCII backend by name:"
be = ts.get_backend_by_name("ascii")
print "Got backend: %d, %s (%s)" % (be.id, be.name, be.enabled)
print

# try to enable the ascii backend
print "Enabling ASCII backend"
print ts.enable_backend(be)
print be
print

# try to set a single value
print "Setting a single value"
print "Should look like: a.test.key 12345 532051200"
print ts.set_single("a.test.key", 12345, 532051200)
print

print "done!"
