Note about Cassandra development, refactoring, etc.

- TODO: Update copyrights, check for any stale HBase comments
- TODO: See whether we can make some of the constructors for system, meta, schema tables private.
- TODO: Add constructor methods for the C* meta, system, schema tables that match those for HBase
- TODO: Optimize all of the get/put value code for the meta, system, schema tables such that we
  generate a prepared statement only once (in the constructor)
- TODO: Get unit tests working for new C* meta, system, schema tables
- TODO: Figure out how we want to organize the unit tests for Kiji-specific stuff
- TODO: Double check that all rows with timestamps are ordered by DESC

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


### Common notes for all of the tables

All three of the tables have the following common methods:

- `public static HTableInterface newFooTable(KijiURI, Configuration, HTableFactory)`.  Creates an
  `HTableInterface` pointing to the HBase table for the system table, schema ID table, schema hash
  table, or meta table.
    - Called internally from the constructor that uses the `HTableInterfaceFactory` (used to get a
      pointer to the `HTableInterface` for calling the other constructor)
    - Called internally from the `install` method for the same reason (get pointers to
      `HTableInterface` methods before calling constructor)
- Constructor `FooTable(KijiURI, Configuration, HTableInterfaceFactory)`.  As described above, uses
  the `HTableInterfaceFactory` and the `newFooTable` static method to get pointers to one or more
  `HTableInterface` objects.
- Constructor `FooTable(KijiURI, HTableInterface)`.  Just assigns the URI and table to members and
  performs some sanity checks (e.g., updating and checking `mState`).
- Installer `static void install(HBaseAdmin, KijiURI, Configuration, HTableInterfaceFactory, some
  map of properties)`.
  This method actually creates the table (instead of just getting a pointer to its
  `HTableInterface`).
- Installer `static void install(HBaseAdmin, KijiURI, Configuration, HTableInterfaceFactory)`.  Just
  calls the other installer, but with an empty map of properties.
- Uninstaller `static void uninstall(HBaseAdmin, KijiURI)`.  Deletes the table.

What calls what?

- As far as I can tell, the *only* place that calls `install` or `uninstall` is `KijiInstaller.`
- Although `newFooTable` is public, the *only* place I see it called is within the `FooTable` class,
  inside the constructor that uses the `HTableInterfaceFactory`.  (*TODO: CAN WE MAKE THIS METHOD
  PRIVATE?*)
- The constructors with the `HTableInterfaceFactory` get called from `HBaseKiji`.
- The constructors that take an `HTableInterface` as an argument get called only within `FooTable`
  and within the unit tests.

We'll keep all of these methods for the Cassandra implementations of these classes.  The main
difference is that we are using `CassandraAdmin` to fill in the role of `HBaseAdmin` and
`HTableInterfaceFactory`.  The methods will therefor look like the following:

- `public static CassandraTableInterface newFooTable(KijiURI, Configuration, CassandraAdmin)`
- Constructor `FooTable(KijiURI, Configuration, CassandraAdmin)`
- Constructor `FooTable(KijiURI, CassandraTableInterface)`
- Installer `static void install(CassandraAdmin, KijiURI, Configuration, map of properties)`
- Installer `static void install(CassandraAdmin, KijiURI, Configuration)`
- Uninstaller `static void uninstall(CassandraAdmin, KijiURI)`


Week of 2014-01-13
==================

Goals for this week:

- Add unit test support for what we did last week
- Add support for meta tables
- Refactor layout.impl code into layout.impl.hbase
- Refine the `CassandraAdmin` and `CassandraTableInterface` classes
  - Start adding more checking, reference counting, etc.
  - Add more documentation (especially with regard to whose responsibility it is to close which
    tables)

New packages:
- o.k.s.layout.impl.cassandra

New files:

- o.k.s.impl.cassandra.CassandraMetaTable
- o.k.s.layout.impl.cassandra.CassandraTableLayoutDatabase
- o.k.s.impl.cassandra.CassandraTableKeyValueDatabase


Unit testing
------------

We are assuming that uses will have an environment with Cassandra, ZooKeeper, and HBase installed.
Ideally we can add C* nodes to whatever fake HBase we are using now.

Unclear now how we should organize the unit tests for the C* implementations of Kiji.  For now, I'll
create a new o.k.s.cassandra package.


Notes on update the meta table
------------------------------

(I did not finish the meta table updates last week.)

We will have two column families, one for the layout-specific metadata and one for user-defined
metadata.  In C*, a column family and a table are the same thing, so we'll also have two
`CassandraTableInterface` members of the `CassandraMetaTable` class (as opposed to the single
`HTableInterface` in `HBAseMetaTable`).

Within the `CassandraTableLayoutDatabase` I implemented straight insertions instead of the
"check-and-insert" that was present in the HBase implementation.  We should be able to implement the
"check-and-insert" stuff pretty easily with the CQL "lightweight transactions."


What to do about HBase "flush" calls?
-------------------------------------

Should these just be C* writes with higher consistency requirements?


