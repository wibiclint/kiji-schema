Note about Cassandra development, refactoring, etc.

Week of 2014-01-06
==================

Goals:

- Refactor as much HBase code as possible into o.k.s.impl.hbase
- Refactor the test code as well, making sure that unit tests still pass
- Create o.k.s.impl.cassandra package and start implementing stuff there
- Be able to install Kiji C* instances
  - Add command-line install, uninstall
  - Implement C* versions of KijiInstaller and the various metadata tables

New packages:

- schema.cassandra
- schema.impl.cassandra
- schema.impl.hbase

New files:

- o.k.s.tools.CassandraInstallTool
- o.k.s.tools.CassandraUninstallTool
- o.k.s.cassandra.KijiManagedCassandraTableName
- o.k.s.cassandra.CassandraKijiInstaller
    - Create keyspace based on KijiURI
    - Create system table, meta table, schema table there
    - For now will not use any factories for these.
    - Keep track of table names in Cassandra keyspaces
- o.k.s.impl.cassandra.CassandraSystemTable
- o.k.s.impl.cassandra.CassandraMetaTable
- o.k.s.impl.cassandra.CassandraSchemaTable


Interfacing with C* tables versus HBase tables
----------------------------------------------

The interface between a client program and an HBase table uses just `byte[]` arrays.  We can mimic
this interface in our C* code by using blobs everywhere.  Doing so, however, makes the CQL shell
somewhat useless, and will probably be strange for experienced C* users.

For now, I'll add lightweight classes that mimic `HBaseAdmin` and `HTableInterface`, but for C*,
just to minimize the amount of refactoring to get C* working.  In the future, we can probably
implement more of the Kiji table layouts with C* primitives, but doing so will require deeper code
changes.


What to do about URIs?
----------------------

For now, let's assume that the host is 127.0.0.1 and ignore the Zookeeper part of the URI.  In the
future, I *think* we could have URIs look the same, but have what is now the Zookeeper information
become instead the information (host name, port) for a bunch of Cassandra nodes in the network in
question.


Testing
-------

For now I am using the BentoBox instead of testing with unit tests.  To do so I had to do the
following:

- Symlink the KijiSchema JAR in the BentoBox lib directory to point to my local kiji-schema build
- I set my KIJI_CLASSPATH to be whatever Maven was using to build KijiSchema (yikes!) using `mvn
  dependency:build-classpath` to print out the classpath.

