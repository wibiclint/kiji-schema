Week-by-week notes
==================

Below are notes about things that I did week-by-week for the first few weeks of the project.  I have
tried to distill everything important here and place it into the `cassandra_notes.md` file.


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


Choosing at run-time between Cassandra and HBase implementations of Kiji components
-----------------------------------------------------------------------------------

One way to implement the run-time choice between creating or references Cassandra or HBase Kiji
implementations is to have the two use different URIs.  A current Kiji URI looks like:

    kiji://(zkHost,zkHost):zkPort/instance

Now, in addition to ZooKeeper hosts, we need to have to allow the user to specify contact points and
a port.  For now, we'll use the following format:

    kiji-cassandra://(zkHost,zkHost):zkPort/(cassandraHost,cassandraHost)/cassandraPort/instance

We shall assume that URIs that don't explicitly start with `kiji-cassandra://` are HBase URIs.

We want to provide an interface to the user in which he can can use the same code to open HBase- or
Cassandra-backed `Kiji` instances, `KijiTable`s, etc., selecting between the two implementations
just by changing the URI.

The point of entry for most anything a user would want to do with KijiSchema is the creation of a
`Kiji` instance.  Currently, there are two ways to get a `Kiji` instance:

- `Kiji.Factory.get().open(kijiURI, ...)`
- `Kiji.Factory.open(kijiURI, ...)`

The former method call we shall have continue to return an `HBaseKijiFactory`, since there is no URI
information present at the time we generate the factory.

The second method call shall do the following:

- Look at the URI to determine whether the factory is HBase- or Cassandra-backed
- Then use the highest-priority `HBaseKijiFactory` or `CassandraKijiFactory` available


Notes on updating system, schema, and meta tables
-------------------------------------------------

#### The system table

Updating the system table looks pretty straightforward.  We just need to create a table with keys
and values as blobs and convert between blobs and byte arrays and we can reuse all of the rest of
the code.


#### The schema tables

The schema tables, one of which stores schemas by IDs, the other of which stores by hashes, are also
pretty straightforward and we can reuse almost all of the code.  A few notes:

- Because the usage of the schema tables is more well-defined (users can put whatever they want into
  the system table), we can use a slightly more sane schema, i.e., everything does not have to be a
  blob.
- For now I'm going to punt on storing every version of every 
- We have to implement counters in the C* style (putting them into a separate table)
- We don't need ZooKeeper locks.  Instead, we can put all of the schema table operations into a
  batch block.


#### The meta table

The meta table stores `KijiTableLayout` information for each user table.

There are two tables in here:

1. `HBaseTableLayoutDatabase` contains the layouts for each table (stored in Avro `TableLayoutDesc`
records)
2. `HBaseTableKeyValueDatabase` contains per-table key-value pairs.  I'm not sure what these are
used for beyond "column annotations."


#### Common notes for all of the tables

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

- o.k.s.cassandra.CassandraFactory
- o.k.s.cassandra.TestingCassandraFactory
- o.k.s.cassandra.CassandraKijiClientTest
- o.k.s.impl.cassandra.CassandraAdminFactory
- o.k.s.impl.cassandra.CassandraKiji
- o.k.s.impl.cassandra.CassandraKijiFactory
- o.k.s.impl.cassandra.CassandraKijiTable
- o.k.s.impl.cassandra.CassandraMetaTable
- o.k.s.impl.cassandra.CassandraTableKeyValueDatabase
- o.k.s.impl.cassandra.DefaultCassandraAdmin
- o.k.s.impl.cassandra.DefaultCassandraAdminFactory
- o.k.s.impl.cassandra.DefaultCassandraFactory
- o.k.s.impl.cassandra.TestingCassandraAdmin
- o.k.s.impl.cassandra.TestingCassandraAdminFactory
- o.k.s.layout.impl.cassandra.CassandraTableLayoutDatabase
- o.k.s.layout.impl.cassandra.CassandraTableSchemaTranslator
- o.k.s.security.CassandraKijiSecurityManager
- o.k.s.tools.CassandraCreateTableTool


Unit and integration testing
----------------------------

We are assuming that uses will have an environment with Cassandra, ZooKeeper, and HBase installed.
Ideally we can add C* nodes to whatever fake HBase we are using now.

For unit tests, we use Cassandra's `EmbeddedCassandraService.`  Much as the HBase implementation of
Kiji has a `TestingHBaseFactory`, we have a `TestingCassandraFactory.`  This factory returns a
`CassandraAdmin` object that will always use a singleton session with the
`EmbeddedCassandraService.`

We need to figure out how we want to clean up Cassandra keyspaces before starting unit tests.  We
don't want to carry over stale C* test sessions from test to test.

Note that Cassandra limits keyspaces to 48 characters (?!), so we have to truncate the names of the
Kiji instances used for tests.
([Apparently](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/config/Schema.java#L49)
this is related to path-size limits in Windows!  Awesome!)

For integration tests, Joe recommends using a Vagrant virtual machine to set up a C* server.

Note that you can run just the C* unit tests at the command line with the command:

    mvn  -Dtest='*Cassandra*' test 



Notes on updating the meta table
--------------------------------

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

#### Table layout descriptions

The current table layout (stored in `TableLayoutDesc` Avro records) has a lot of HBase-specific
stuff.  We may want to make a separate description for C*-backed Kiji tables.

HBase offers a very nice data structure, `HTableDescriptor`, that contains all of the layout
information for an HTable.  I'm not sure if we have anything similar for Cassandra.  The mapping
between Kiji tables and C* tables is not as straightforward as that between Kiji tables and HTables,
so we may need to make big changes in how we handle the Kiji / C* translation.


#### Entity ID factory

We don't have to do all of the hashing, etc. within Kiji anymore.  We could do so, and just make
every entity ID a C* blob, but doing so would really reduce the readability of the C* table to
anyone familiar with C* (and make debugging a lot harder).


#### Updating table layouts

All of the synchronization / update-passing stuff related to updating table layouts, e.g., 

- `InnerLayoutUpdater`
- `LayoutTracker`
- `LayoutCapsule`
- `LayoutConsumer` (I'm assuming a `LayoutConsumer` is a table reader or writer)

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
- `CassandraTableLayoutUpdater` (Seems to be used for ZooKeeper to broadcast table layout updates)
- `CassandraTableSchemaTranslator`
  - I don't know if we'll have the equivalent of `ColumnNameTranslator` because the mapping from
    Kiji column to C* will be so different from what you would get in HBase.
  - This is used only within `HBaseKiji`


Week of 2014-01-20
==================

Goals for this week:

- Continue to refine the `CassandraAdmin` and `CassandraTableInterface` classes
  - Reference counting
  - Documentation
- Start actually implementing C* Kiji tables
  - Support creating tables, but not altering them
  - Support simple reads (no row scanning)
  - Support writing data and simple deletes (delete a row or a single cell)


New classes and packages
------------------------

- `o.k.s.impl.cassandra.CassandraKijiTable`
- `o.k.s.impl.cassandra.CassandraKijiReaderFactory`
- `o.k.s.impl.cassandra.CassandraKijiWriterFactory`
- `o.k.s.impl.cassandra.CassandraKijiTableReader`
- `o.k.s.impl.cassandra.CassandraKijiTableReaderBuilder`
- `o.k.s.impl.cassandra.CassandraKijiTableWriter`
- `o.k.s.impl.cassandra.CassandraKijiTableWriter`
- `o.k.s.impl.cassandra.CassandraDataRequestAdapter`
- `o.k.s.impl.cassandra.CassandraKijiRowData`
- `o.k.s.CassandraKijiURI`


How to store Cassandra table layout information?
------------------------------------------------

There does not appear to be any Cassandra analogue (at least in the DataStax Java driver) for
HBase's `HTableDescriptor`, which contains a structured description of the layout of an HBase table.
We can sort of replace it with `TableMetadata`, which we can generate for an existig table, but we
cannot build or modify our own `TableMetadata` instances, and we cannot use a `TableMetadata`
instance to update the layout of an existing table.

Where to we use `HTableDescriptor` now?  (Omitting usages that are easy to replace)
- `HBaseKiji` for modifying table layouts
  - The table description comes from `HTableSchemaTranslator` translating a `TableLayoutDesc`
  - Code goes through every column in the table, making updates if the new column is different from
    the previous column
- `HBaseKiji` for creating a new table
  - The table description comes from `HTableSchemaTranslator` translating a `TableLayoutDesc`
- `HTableSchemaTranslator` translates from `TableLayoutDesc` to `HTableDescriptor`
- `HTableDescriptorComparator` compare two HTable layouts (checks to see if any layout updates are
  actually necessary)

It might be worthwhile copying the `TableMetadata` and associated code from the DataStax source,
stripping it down a bit, and just using that.


Total functionality for C* / Kiji interface
-------------------------------------------

- Creating and altering tables
  - Given a `TableLayoutDesc`, create a C* table or set of tables
  - Given a `TableLayoutDesc` and an existing C* table, alter the existing table to implement the
    new layout
- Reading data
  - Normal "get"
    - Given a `KijiDataRequest`, an `EntityId`, and a `KijiTableLayout`, create a C* `SELECT`
      statement to query the database.
    - Given the result of the query, create an instance of `KijiRowData`
  - "Bulk get"
    - Do the same (create query, return `KijiRowData`) given a list of `EntityId` instances
  - scan
    - Given a `KijiDataRequest`, a `KijiTableLayout`, and start and stop `EntityId`s, create a
      row-scanner `SELECT` statement
    - Return a `KijiRowScanner`
- Writing data
  - Given an `EntityId`, column family, column qualifier, timestamp, and value, write to the
    database
  - Given an `EntityId`, column family, qualifier, and amount, increment a counter by amount
  - Given a column family and qualifier, verify that the column is a counter
- Deleting data
  - Given an `EntityId`, delete a row
  - Given an `EntityId` and a timestamp, delete all values up to that timestamp
  - Given an `EntityId`, column family, and timestamp, delete all values in that family up to the
    timestamp
    - Requires a row lock for a map-type family in HBase
  - Given an `EntityId`, a family, a qualifier, and maybe a timestamp, delete values
- Row data
  - `KijiRowData` has lots and lots of different methods for viewing results, but most of them just
    use `mFilteredMap` (a map from family to qualifier to timestamp to value) in different ways.
  - `KijiRowScanner` is really just an iterator of `KijiRowData`.  Should not present any additional
    translation difficulties.

It might make sense to put all of this functionality (for now) into a `CassandraLayoutTranslator`
object or something like that.  Keeping all of the Kiji/C* mapping code in one place would make
iterating easier.


Layout capsules and other such stuff
------------------------------------

`o.k.s.impl.LayoutConsumer` has a signature that expects an
`o.k.s.impl.hbase.HBaseKijiTable.LayoutCapsule`.  We need to fix this.

Probably easiest to create a new `LayoutCapsule` interface and have the
`o.k.s.impl.hbase.HBaseKijiTable.LayoutCapsule` and
`o.k.s.impl.cassandra.CassandraKijiTable.LayoutCapsule` both implement it.  This should be easy,
just has a bunch of getters in it.


Implementing gets
-----------------

Reads are tricky for the C* Kiji because we do not have a 1:1 correspondence between C* tables and
Kiji tables.  For each Kiji read, we'll need to have one C* read per colum family.  How does this
change affect the code?  One proposal:

- `CassandraDataRequestAdapter` creates a list of Cassandra `Statement`s for each `DataRequest`
- The `CassandraKijiTableReader` issues those statements to a Cassandra `Session` and get back a
  list of Cassandra `ResultSet`s
- Combine those result sets into a `KijiRowData` instance
- We need to think a little bit about how to prepare these query statements only once, even across
  multiple `CassandraDataRequestAdapter` instances.

If everything fits into memory, then this works out great.  But what do we do about paging?

We will get back on DataStax `ResultSet` per query.  We can think of each `ResultSet` as being
potentially almost bottomless, since we could be querying a column family with lots and lots and
lots of qualifiers and versions.

If we are not using paging, then we can assume that everything fits into main memory and so we can
go through every `Row` in every `ResultSet` for a given get request and use the results to fully
populate the big nested `NavigableMap` in `KijiRowData`.


Implementing a row scanner
--------------------------

A row scanner provides an interable interface to a multi-row read from a Kiji table.  The results of
the multi-row read are *not* guaranteed to fit into main memory (that is kind of the point of the
row scanner!).

To implement a row scanner, we need to know that all of the `Row` objects in all of the `ResultSet`
objects that we get back from all of the various queries we perform on a C* table are grouped by
entity ID -- this is fine because our entity ID is our Cassandra partiion key.  The algorithm for a
row scanner therefore looks something like this:

    Perform a bunch of Cassandra queries
    Get back on ResultSet object per query (per fully-qualified column or per column family)
    do:
      create an empty KijiRowData
      eid = the earliest entity ID from all of the ResultSet objects
      for (each resultSet in all ResultSets)
        add all data for eid to the current KijiRowData
        (stop when you get to a new eid)
      emit the KijiRowData
    while we still have data left in the ResultSet objects

The tricky part is that we need a way to tell when we have gotten to the end of the data for a given
entity ID for a `ResultSet`, likely when we are using the `ResultSet` as an `Iterator<Row>`.  To
find that we've gotten the last piece of data for an entity ID, we'll have to read a piece of data
with the new entity ID, which means we will have pulled that data off of the iterator.  So we'll
need to know how to save the first `Row` of data for each `ResultSet` for each entity ID.  (Note:
Google's `common.collect` has a `PeekingIterator`, awesome!!!!!)

We can share the code that reads through a bunch of `ResultSet`s while the eids are the same and
creates a `KijiRowData` instance between the functions for gets and row scanners.  We could probably
even implement a get under the hood with a row scanner, since the underlying implementations won't
be very different now.

What if you want to mix paging with a row scanner?  What if a user is going through every row in his
table, and fetching data from one column family with a small amount of information, and another with
a potentially HUGE amount of information.  He uses paging on the second family, and, depending on
the contents of the first family, may or may not wish to page through all of the data in the second.
Can we supporting this use model without paging in the background through *all* of the data for the
second family?  We can certainly support this functionality if we do our own manual paging, instead
of using the paging that the DataStax driver allows.



DataStax driver paging
----------------------

Paging of results from the DataStax Java driver should happen automatically.  At most, we should
have to set the fetch size by using the `setFetchSize` method in class `Statement`.  There are also
methods like `getAvailableWithoutFetching`, `isFullyFetched`, and `fetchMoreResults` that we can use
to have more manual control over paging.

There is a little bit more information in
[this blog post](http://www.datastax.com/dev/blog/client-side-improvements-in-cassandra-2-0)
about Cassandra 2.0.

The API documentation for `fetchMoreResults` also has some example code (`fetchMoreResults` can be
used to prefetch more data).


Functionality needed for the phonebook tutorial
-----------------------------------------------

- Add C*-specific URIs
- The DDL shell may need to be modified to call C* code (need to check)
- Delete a Kiji instance
- `AddEntry.java` - Requires doing puts
- `Lookup.java` - Requires doing gets
- `StandalonePhonebookImporter` - Also just does puts
- `PhonebookImporter` - Use KijiMR, not ready yet
- `AddressFieldExtractor` - Appears to be deprecated, calls a method `getMostRecentValue` that I
  don't think exists anymore
- `IncrementTalkTime` - Uses counters, but generally seems deprecated for KijiMR stuff.
- `DeleteEntry` - Calls delete.


Prepared statements for different tables.
-----------------------------------------

For now, I am putting a Map from tableURI-to-prepared statements for reading, writing, etc. into
`CassandraAdmin`

Week of 2014-02-24
==================

Paging
------

We need to support paging in C* Kiji.  Milo started circulating a document with proposals for
KijiSchema 2.0, which will be the first version to support Cassandra.  Given that we are going to
change the major version of KijiSchema for C* support, we may not need to support 100% of the
current KijiSchema paging API (since we can change it in 2.0), but we should try.

The main trick with paging is likely that we cannot build up the entire
family-to-qualifier-to-timestamp-to-value map structure in `KijiRowData` in one go now, since all of
the cells may not fit into memory.  The current code will iterate through everything and create a
map with all of the data for the entire row.

In the current KijiSchema, it looks like paging works like this:

- The user specifies that he wants to use paging when he creates his data request
- The user then has to use "getPager" for the family or qualified column
- The pager has an interface that looks like an iterator over `KijiRowData`.

The user can also get different iterators over a column that hide the page-fetching process:

- `ColumnVersionIterator` iterates through the versions in a given column.
- `MapFamilyQualifierIterator` iterates through the qualifiers in a map-type family.
- `MapFamilyVersionIterator` iterates through the versions in all columns from a map-type family.

In all of these, the initial `KijiRowData` does not contain any values for any columns that have
paging enabled.  The subsequent calls to `ColumnVersionIterator`, `KijiPager`, etc. make multiple
get requests to get the paged data.

We may need to refactor how we organize the various calls to
`CassandraDataRequestAdapter#queryCassandraTables`.  We have the following cases:

- Standard scans and gets
  - Should *not* try to get any data from a Cassandra table for any paged columns.
  - The paged columns will get requested later by a `KijiPager`
- Paged gets (I don't think there are paged scans)
  - We should be able to verify that *every* column in a `KijiDataRequest` used for a paged request
    has paging turned on.
  - We can probably do other assertion checks for the `KijiDataRequest` for a paged operation, e.g.,
    that it should not have more than one column (or at least more than one family).
  - A paged get should (for now) return only a single ResultSet, not a collection of them.

##### Qualifier pager

The code for this is pretty simple, but would be a lot simpler if Cassandra had a way to filter out
duplicate results in a query on the server.  To get a list of all of the qualifiers for a column, we
have to do something like:

    SELECT qualifier, version FROM <table> WHERE key=<key> AND family=<family> AND qualifier <=
    <maxqual> AND qualifier >= <minqual>

We select `version` so that we can filter out any results for which the timestamp is out of the
bounds specified in the user's data request.  We could put the timestamp range in the query as we
have put the qualifier range into the query, starting with C* 2.0.6 (see this
[ticket](https://issues.apache.org/jira/browse/CASSANDRA-4851)).

In any case, we'll get back a separate qualifier result for every cell that has the qualifier that
we're looking for.



...

The iterator stuff was not that hard to get done.  The C* iterator code actually is simpler than the
HBase iterator code, just because the DataStax Java driver does so much of the work.

The HBase implementation of the map family version iterator works like this:

- Get a paged iterator of every qualifier in the map family
- For each qualifier, we a paged iterator over cells

If we can figure out a way to support max versions in our queries, then we can probably build one
entire big query for the entire multi-qualifier, multi-version pager.  Doing so would possibly allow
us to save a lot of RPCs, since many qualifiers may not have enough versions to max out a version
pager by themselves.
