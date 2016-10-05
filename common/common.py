#!/usr/bin/python

"""
Common data definitions module for all of the tools.  Stores data that is required or useful for >1 tool in the
looms set.  The common module should be on the natural search path of the python 2.7 interpreter on the host system,
or else a root configuration file should contain a reference to the module's directory so that it can be added to
the system search path.
"""

# This variable is available to any module to provide a version information.  An installer module, for instance,
# can import the common module of an existing install to determine what changes may need to be made to the db schema
# or ansible playbook set, rather than have to scope out each service individually.
version = "0.000001a"

