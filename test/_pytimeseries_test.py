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

# try to enable the ascii backend with options
print "Enabling ASCII backend (with ignored options):"
print ts.enable_backend(be, "ignored options")
print be
print

# enable the ascii backend
print "Enabling ASCII backend:"
print ts.enable_backend(be)
print be
print

# try to set a single value
print "Setting a single value:"
print "Should look like: a.test.key 12345 532051200"
print ts.set_single("a.test.key", 12345, 532051200)
print

# create a key package
print "Creating 3 Key Packages:"
print ts.new_keypackage(True)
print ts.new_keypackage(False)
kp = ts.new_keypackage(reset=True)
print kp
print

# add key to key package
print "Adding Key to Key Package ('a.test.key'):"
print "Should return 0"
print kp.add_key("a.test.key")
print "Adding 'another.test.key', should return 1:"
print kp.add_key("another.test.key")
print "Getting index of 'another.test.key', should return 1:"
print kp.get_key("another.test.key")
print "Getting index of 'a.test.key', should return 0:"
print kp.get_key("a.test.key")
print "Disabling 'a.test.key', should return None:"
print kp.disable_key(kp.get_key('a.test.key'))
print "Enabling 'a.test.key', should return None:"
print kp.enable_key(kp.get_key('a.test.key'))
print "Getting the current value of 'a.test.key', should return 0:"
print kp.get(kp.get_key('a.test.key'))
print "Setting the current value of 'a.test.key' to 12345:"
print kp.set(kp.get_key('a.test.key'), 12345)
print "Getting the current value of 'a.test.key', should return 12345:"
print kp.get(kp.get_key('a.test.key'))
print "Forcing resolution of all keys, should return None:"
print kp.resolve()
print "Getting the number of keys, should return 2:"
print kp.size
print "Disabling 'another.test.key' and getting enabled size, should return 1:"
kp.disable_key(kp.get_key('another.test.key'))
print kp.enabled_size
print "Flushing key package, should output 1 line of metrics and then None:"
print kp.flush(532051200)
print "Enabling 'another.test.key' and flushing, should output 2 lines of data:"
kp.enable_key(kp.get_key('another.test.key'))
kp.flush(532051200)
print


print "done!"
