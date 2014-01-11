Note about Cassandra development, refactoring, etc.

- TODO: Update copyrights, check for any stale HBase comments
- TODO: See whether we can make some of the constructors for system, meta, schema tables private.
- TODO: Add constructor methods for the C* meta, system, schema tables that match those for HBase
- TODO: Optimize all of the get/put value code for the meta, system, schema tables such that we
  generate a prepared statement only once (in the constructor)
- TODO: Get unit tests working for new C* meta, system, schema tables

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

- New classes to provide similar functionality to HBaseAdmin and HTableInterface
  - o.k.s.impl.cassandra.CassandraAdmin
  - o.k.s.impl.cassandra.CassandraTableInterface


Interfacing with C* tables versus HBase tables
----------------------------------------------

The interface between a client program and an HBase table uses just `byte[]` arrays.  We can mimic
this interface in our C* code by using blobs everywhere.  Doing so, however, makes the CQL shell
somewhat useless, and will probably be strange for experienced C* users.

For now, I'll add lightweight classes that mimic `HBaseAdmin` and `HTableInterface`, but for C*,
just to minimize the amount of refactoring to get C* working.  In the future, we can probably
implement more of the Kiji table layouts with C* primitives, but doing so will require deeper code
changes.


Code reuse
----------

For a lot of KijiSchema components, we have an interface and then an HBase class that implements
that interface.  Many of the C* classes that we are implementing now will share a lot of code with
the HBase classes.  Do we want to add another level of indirection to allow us to not copy/paste
code between HBase and C*?  e.g., we could do

    KijiFoo [interface] -> AbstractKijiFoo [abstract class] -> HBaseKijiFoo / CassandraKijiFoo


What to do about URIs?
----------------------

For now, let's assume that the host is 127.0.0.1 and ignore the Zookeeper part of the URI.  In the
future, I *think* we could have URIs look the same, but have what is now the Zookeeper information
become instead the information (host name, port) for a bunch of Cassandra nodes in the network in
question.

We could also temporarily make C* URIs look like: `kiji-cassandra/.env/default`.  By doing so, we
could possibly add a lightweight way of implementing the C* / HBase bridge functionality internally
(e.g., any time you are trying to get a `KijiTable` instance, you check the URI to see if it is a C*
or HBase URI and then proceed appropriately).


Testing
-------

For now I am using the BentoBox instead of testing with unit tests.  To do so I had to do the
following:

- Symlink the KijiSchema JAR in the BentoBox lib directory to point to my local kiji-schema build
- I set my KIJI_CLASSPATH to be whatever Maven was using to build KijiSchema (yikes!) using `mvn
  dependency:build-classpath` to print out the classpath.


Notes on updating system, schema, and meta tables
-------------------------------------------------

### The system table

Updating the system table looks pretty straightforward.  We just need to create a table with keys
and values as blobs and convert between blobs and byte arrays and we can reuse all of the rest of
the code.


### The schema tables

The schema tables, one of which stores schemas by IDs, the other of which stores by hashes, are also
pretty straightforward and we can reuse almost all of the code.  A few notes:

- Because the usage of the schema tables is more well-defined (users can put whatever they want into
  the system table), we can use a slightly more sane schema, i.e., everything does not have to be a
  blob.
- For now I'm going to punt on storing every version of every 
- We have to implement counters in the C* style (putting them into a separate table)
- We don't need ZooKeeper locks.  Instead, we can put all of the schema table operations into a
  batch block.


### The meta table

The meta table stores `KijiTableLayout` information for each user table.

There are two tables in here:

1. `HBaseTableLayoutDatabase` contains the layouts for each table (stored in Avro `TableLayoutDesc`
records)
2. `HBaseTableKeyValueDatabase` contains per-table key-value pairs.  I'm not sure what these are
used for beyond "column annotations."




