Note about Cassandra development, refactoring, etc.

Open TODOs
==========

- TODO: Update copyrights, check for any stale HBase comments
- TODO: See whether we can make some of the constructors for system, meta, schema tables private.
- TODO: Add constructor methods for the C* meta, system, schema tables that match those for HBase
- TODO: Optimize all of the get/put value code for the meta, system, schema tables such that we
  generate a prepared statement only once (in the constructor)
- TODO: Get unit tests working for new C* meta, system, schema tables
- TODO: Figure out how we want to organize the unit tests for Kiji-specific stuff
- TODO: Double check that all rows with timestamps are ordered by DESC
- TODO: I think the methods in `KijiManagedCassandraTableName` could be a little bit more explicit
  about whether they are returning names in the Kiji namespace or in the C* namespace.
- TODO: Add super-unstable annotations to this API.  :)


Open questions
==============

(This is kind of a random list.  There are other open items on the spreadsheet in google docs.)

- Do we want to have a common `TableLayoutDesc` for HBase- and C*-backed Kiji tables?  There is a
  lot of HBase-specific stuff in there now.  Things we could put into a C*-specific layout
  description:
    - Query patterns.  e.g., for a given column family, do we want to search for all versions for a
      given qualifier, or all qualifiers for a given version (e.g., the most recent)?  (This choice
      affects the order of the columns in the composite primary key.)
- The same goes for data requests.  Presumably we need some way to expose the C* consistency levels
  to the user.
- *Code reuse:* Do we want to do a broader refactoring of the interfaces and classes in
  `o.k.s.impl.hbase` and `o.k.s.impl.cassandra` to share more code between the HBase and C*
  implementations?  Many of the pairs of HBase/C* classes share probably 80-90% of the code, with
  the only differences existing in the methods that actually interact with the underlying tables.
  (Scala's traits would be perfect for this, since we could make the interfaces into traits and put
  the common code there...)
- How do we want to implement permissions?
- Do we want to offer any kind of reduced Kiji functionality for users that have Cassandra set up,
  but don't have ZooKeeper installed?


Mapping Kiji to Cassandra
=========================

Below is an early draft of a way to translate Kiji tables into Cassandra.


Kiji table
----------

A Kiji table corresponds to a group of C* tables (previously known as C* "column families"), all of
which share the same row key.


Locality groups
---------------

For now, we can skip implementing locality groups.  I'm not sure how to implement these later on.
As far as I can tell, we have pretty good control over how data within a single C* table is
structured (by using compound primary keys and composite partition keys), but beyond that I am nto
sure what we can do.

### Whether to store locality group information in memory or on disk

In a talk from the C* Summit EU 2013, one of the speakers [describes how to ensure that Cassandra
keeps some tables in
memory](http://www.slideshare.net/planetcassandra/c-summit-eu-2013-playlists-at-spotify-using-cassandra-to-store-version-controlled-objects)
(see slide 32/37).  The bottom line from his talk was that Cassandra's row cache did not work.
Maybe this is fixed in Cassandra 2.x.

### Data retention lifetime

In Kiji, we specify TTL at the locality-group level.  CQL allows users to specify a TTL when
inserting data into a table.  We can allow users of C* Kiji to specify TTL for locality groups as
they do now and propagate that information down to every insert we perform.

### Maximum number of versions to keep

I'm not sure of a simple way to implement this in Cassandra.  We could have a counter for every
fully-qualified column that keeps track of the total number of versions, and, once we have the max
number of versions for the fully-qualified column, we could delete an older version for every new
version that we add (in practice we'd probably want to have a threshold for how many stale values we
leave in the table before we clean up).

See the section below with CQL for implementing column families for a note about what to do if the
max number of versions is one.

### Type of compression

CQL allows you to specify `LZ4`, `Snappy`, and `DEFALTE.`  The DataStax CQL documentation has a lot
of details about the compression options.

### Bloom filter settings

CQL allows you to set the `bloom_filter_fp_chance` property for a table to tune the chance of
getting a false positive from your bloom filter.


Map-type families
-----------------

We can implement map-type families in the same way in C*.  For every family, we have *two* C* tables
that looks like the following:

    CREATE TABLE kiji_table_name_column_family_name_qual_time (
      first_row_key_field text,
      second_row_key_field int,
      qualifier text,
      time timeuuid,
      value blob,
      PRIMARY KEY ( (first_row_key_field, second_row_key_field), qualifier, time)
    ) WITH CLUSTERING ORDER BY (time DESC);

    CREATE TABLE kiji_table_name_column_family_name_time_qual (
      first_row_key_field text,
      second_row_key_field int,
      qualifier text,
      time timeuuid,
      value blob,
      PRIMARY KEY ( (first_row_key_field, second_row_key_field), time, qualifier)
    ) WITH CLUSTERING ORDER BY (time DESC);

The two tables are identical except for the order of the composite partition keys.  The first table
allows us to make a data request that selects a range of timestamps for a given qualifier.  The
second table allows ut o make a data request for the latest version of every qualifier.

To begin with, we likely want to implement only one of these tables.  Having two tables like this
means that we'll have to use batch commands for writes to ensure that the two views of the same
table stay in-sync.

Note that this example shows how to use Cassandra's compound row keys.

If the max number of versions for a locality group is one, then we can use a single C* table per
column family in the locality group, and we can remove the timestamp from the composite partition
key.  By removing the timestamp from the partition key, insertions for a given row key and qualifier
will always overwrite previous insertions, so we will have only one version in the table at any
time.


Group-type families
-------------------

We can likely use the map-type family implementation described above for group-type families.  In an
ideal world, we would be able to do some optimizations for group-type families such that we add
dedicated Cassandra table columns for every qualifier in the group-type family.  Doing so would
allow us to map directly between primitive types in Kiji and Cassandra without using Avro and blobs.


Row keys
--------

Currently we limit the fields in row keys to strings, ints, and longs.  We can pretty easily
implement Kiji row keys by using Cassandra's compound row keys.

### Using null in row keys

I am pretty sure that we cannot do this if we use Cassandra's compound row keys.  I believe that an
insert has to specify a value for every field in the row key.


Kiji cell schemas
-----------------

I'd like to not support any of the legacy cell schemas (`INLINE`, `CLASS`) and support only
`COUNTER` and `AVRO`.


CQL collections
---------------

CQL has nice built-in support for lists, maps, and sets.  I chose not to use them above, however,
for a couple of reasons:

- Any query that selects a collection returns the entire collection (no paging)
- The collections max out at 64K entries


Other stuff we may want to expose
---------------------------------

- Compaction strategies
- Caching



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
- Begin looking at how to implement Cassandra KijiTables.

New packages:
- o.k.s.layout.impl.cassandra

New files:


- o.k.s.impl.cassandra.CassandraKiji
- o.k.s.impl.cassandra.CassandraKijiFactory
- o.k.s.impl.cassandra.CassandraKijiTable
- o.k.s.impl.cassandra.CassandraMetaTable
- o.k.s.impl.cassandra.CassandraTableKeyValueDatabase
- o.k.s.layout.impl.cassandra.CassandraTableLayoutDatabase
- o.k.s.layout.impl.cassandra.CassandraTableSchemaTranslator
- o.k.s.security.CassandraKijiSecurityManager
- o.k.s.tools.CassandraCreateTableTool


Unit testing
------------

We are assuming that uses will have an environment with Cassandra, ZooKeeper, and HBase installed.
Ideally we can add C* nodes to whatever fake HBase we are using now.

Unclear now how we should organize the unit tests for the C* implementations of Kiji.  For now, I'll
create a new o.k.s.cassandra package.

Joe suggests using a Vagrant virtual machine that can run a C* server that the unit tests can use.
Another option is using Cassandra's `EmbeddedCassandraService.`


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


Security
--------

I did not put `CassandraKijiSecurityManager` into
o.k.s.security.impl.cassandra.CassandraKijiSecurityManager because there are so many package-private
security things that it needs to access.  I assume this code is going to have to get rewritten
anyway, so I did the minimum to get it to compile for now.


Notes on support for creating C*-backed Kiji tables
---------------------------------------------------

Summary of stuff that we'll have to change:

### Table layout descriptions

The current table layout (stored in `TableLayoutDesc` Avro records) has a lot of HBase-specific
stuff.  We may want to make a separate description for C*-backed Kiji tables.

HBase offers a very nice data structure, `HTableDescriptor`, that contains all of the layout
information for an HTable.  I'm not sure if we have anything similar for Cassandra.  The mapping
between Kiji tables and C* tables is not as straightforward as that between Kiji tables and HTables,
so we may need to make big changes in how we handle the Kiji / C* translation.


### Entity ID factory

We don't have to do all of the hashing, etc. within Kiji anymore.  We could do so, and just make
every entity ID a C* blob, but doing so would really reduce the readability of the C* table to
anyone familiar with C* (and make debugging a lot harder).


### Updating table layouts

All of the synchronization / update-passing stuff related to updating table layouts, e.g., 

- InnerLayoutUpdater
- LayoutTracker
- LayoutCapsule
- LayoutConsumer (I'm assuming a `LayoutConsumer` is a table reader or writer)

...can probably stay the same.  This is a pretty large portion of the code in `HBaseKijiTable.`


### Reader / Writer factories

These seem to allow the user to override the default encoding and decoding of data going to/from
tables.  I suggest punting on these for now.


### Table annotator?

This provides a way to work with the user-defined key-value pairs in the meta table.  Should be
pretty easy to get this to work with Cassandra.


### Bulk load

Loading HFiles into a C* does not make sense (AFAIK).  Possibly we will want to offer functionality
similar to this in the future, but with bulk loading of SSTables.


### Regions

Don't really make sense in the context of a C* implementation.


### `KijiTableWriter`

This looks pretty straightforward and should map well to C*.  It looks like this is where the rubber
meets the road and we really implement the Kiji / HBase interface for writes.  Here is where we will
put a lot of the C* code that implements writes to the C* tables.

In the `KijiTableWriter` we are mostly just dealing with puts of single cells, so we shouldn't have
to deal too much with the messiness of having one C* table per Kiji column family.


### `KijiTableReader`, `KijiRowData`, `KijiRowScanner`, `HBaseRequestAdapter`

This is where a lot of the action will happen.  We have to think carefully about how we encapsulate
the code that maps between Kiji and C*.

A lot of the code in `KijiTableReader` again concerns how to synchronize table layouts and handle
different cell decoders.  This code should not have to change.


So where does all of the C* / Kiji interface code go?
-----------------------------------------------------

Translations between C* and Kiji need to happen in the following places:

- Any updates to table layouts (including creating a table layout)
- Translating Kiji writes in C* writes
- Translating Kiji reads in C* queries
- Translating C* query results into Kiji read results
- (We also do some C* stuff when we create Kiji instances and create the system, meta, and schema
  tables, but that is decoupled from the other operations.  We should easily be able to change how
  we represent any of these extra tables in C* without changing anything else in Kiji.)

It would be good to keep all of this translation code as localized as possible so that we can easily
make changes to how we implement Kiji under the hood in C*.

Here is a list of places where we'll have to put changes:

- `CassandraKijiTableReader`
- `CassandraKijiTableWriter`
- `CassandraKijiRowData`
- `CassandraKijiRowScanner`
- `CassandraRequestAdapter`
- `CassandraKijiTable`
- `CassandraKiji` (this contains a lot of the code for implementing updates to table layout)
- `CassandraTableSchemaTranslator` (I don't know if we'll have the equivalent of
  `ColumnNameTranslator` because the mapping from Kiji column to C* will be so different from what
  you would get in HBase.
