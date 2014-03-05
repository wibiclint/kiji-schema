Notes about Cassandra development, refactoring, etc.
====================================================

This document includes notes about the C* versions of KijiSchema and KijiMR (almost all of the code
to support C* in Kiji is in KijiSchema).  I moved the super-detailed notes of everything that I did
every week into `cassandra_weekly_notes.md`.

Open TODOs
==========


### Major missing features

- Bulk importers!

- Security / permission checking is not implemented at all now.

- Add support for filters (even if everything has to happen on the client for now).
  - We may also need to modify the KijiColumnFilter interface to have a method like:
    `filterCell(qualifier, family, timestamp, value)`.
  - The filters are going to need additional getter methods so that the C* code can get information
    about the filters (e.g., the min and max qualifiers for a column range filter).

- We need a C* version of `KijiTableAnnotator`.

- Compare-and-set in the C* `AtomicKijiPutter`
  - All non-compare-and-set functionality is complete
  - CQL has support for "lightweight transactions" that perform compare-and-set, but it does not
    currently support batch operations.
  - I started a
    [thread](http://mail-archives.apache.org/mod_mbox/cassandra-user/201402.mbox/%3CCAKkz8Q3Q9KC0uhX5-XmZ4w8HXyL8Bt_-A1iJbn3xGW7uYvJ0xw%40mail.gmail.com%3E)
    about this on the users list.  C* 2.0.6 will have some additional support for batch
    compare-and-set via static columns.

- There are still some HBase-specific classes that need to be refactored into `impl` packages.

- Support for scanning over primary key ranges
  - To implement this, we need to use an order-preserving hash function in Cassandra (which all of
    the C* documentation discourages).

- TTL, keeping parts of a database in-memory, etc.

- Integration tests (especially those with multiple Cassandra nodes)

- Integration tests with KijiMR


### General cleanup:

- Update copyrights, check for any stale HBase comments
- Add super-unstable annotations to this API.  :)
- Remove / sanitize the huge number of LOG.info messages.


### Prepared CQL statements:

- Update any class that calls `Session#execute` to use a prepared query.


### Testing and error messages

- It might be useful to have a notion of a read-only table.  Such a table would be great for unit
  tests, because we could initialize a shared table once and then share it across a lot of read-only
  tests.  This would probably speed up the tests a lot!
- Need comprehensible error message if the user does not have Cassandra running (need an error
  message that is better than a stack trace)
- Desperately need more unit tests for the Cassandra / Hadoop code that I wrote, also for the
  record reader and input format in Kiji MR.
- Add integration tests
  - Especially need tests with multiple Cassandra nodes to make sure that the new Cassandra / Hadoop
    code in KijiMR and in the Cassandra2/Hadoop2 interface works properly.
- The `CassandraKijiClientTest` needs to allow a user to use a JVM system property to force unit
  tests to run on a real Cassandra server (and not on the session created within the unit tests).
- The code in `CassandraKijiRowScanner` that creates `KijiRowData` instances from a list of
  iterators over `Row`s needs *a lot* of testing.  This code has to create the results of multiple
  RPC calls to provide a coherent view of a given Kiji row.  There is some trickiness that can occur
  if some Kiji rows do not have any data present whatsoever for a given column.  I had an e-mail
  thread with Joe about this and there is a `token` function that we use to order the entity IDs.
  I added some unit tests, but more (especially integration tests) would be useful.
- Need unit tests for the command-line tools


### General code organization

- Before adding C* support, most of the schema components had an interface and a single implementing
  class.  Now we have an interface and two implementing classes (one for HBase, one for C*).  In
  most cases, the two implementing classes share *a lot* of code.  We may want to refactor all of
  the shared code into an abstract superclass from which both the HBase and C* implementations can
  inherit.

- Think about limiting the number of places from which we can call `Session#execute`.
  - Might be good to put all of these calls within `CassandraAdmin`, for example.
  - That would make it easier to manage prepared queries, since the queries are prepared
    per-`Session` (double-check that this is true) and the `CassandraAdmin` manages the active
    Cassandra `Session`.
  - Such a refactoring would also make it easier to modify the way in which we map Kiji to C*, since
    we'd likely have to change only a single file.
  - We could remove the `getSession` method entirely from `CassandraAdmin` if we want to really
    tighten this up.

- Do we want to refactor `CassandraAdmin` into multiple classes?  Should we add or remove some
  functionality to or from it?  The class as it is now is somewhat arbitrary -- I created it mostly
  as a wrapper around an open Cassandra `Session` to try to future-proof the code.

- Do we need the `CassandraTableInterface` class?  It is basically just a table name and a pointer
  to a `CassandraAdmin` (which may be useful by itself).
  - We may want to keep this class around to manage reference counting for open sessions.
  - We could have the various objects that use CQL always go through an instance of
    `CassandraTableInterface`, rather than getting a `Session` from `CassandraAdmin`.  Such a
    restriction would make it easier to manage what objects are doing what with the currently open
    `Session.`

- The DataStax Java driver has builder methods for creating queries.  I could not find much
  documentation about them, however, so I have not used them yet.  Using builders would make some of
  the code a lot more compact (e.g., in cases in which we have to conditionally add clauses to a
  query).

- The code for column-name translation is kind of a mess right now.  I wanted to share the layout
  capsule code as much as possible between the HBase and Cassandra Kiji implementations.  The
  layout capsule contains a `ColumnNameTranslator` already, so to share that code, but to enable a
  different translator for Cassandra Kiji, I had to make `CassandraColumnNameTranslator` a subclass
  of `ColumnNameTranslator`, which is okay, but really they should both be separate implementations
  of a much smaller interface (mostly likely).  Yikes!  In the interest of making more progress, I
  left this code ugly for now and moved on, but we need to clean this up later (and possibly reorg
  it).


### Performance

- We may be doing a lot of unnecessary conversions from `ByteBuffer` to `byte[]` to final data types
  when we read data back from Cassandra tables.  (I have not looked into this much, but we should
  check.)

- We can use the asynchronous `Session#executeAsync` method whenever we are issuing multiple
  requests for data to the Cassandra cluster.

- Add some performance tests!
  - How does this perform versus HBase?
  - How does this perform versus bare-bones Cassandra?

- The `CassandraKijiBufferedWriter` should be aware of the replica nodes for different key ranges
  and send requests directly to the replica nodes.

- We can likely improve the implementation of the `MapFamilyPager` for Cassandra.  The HBase
  implementation works like the following:

  - Create a paged iterator of column qualifiers (for a given family)
  - For each qualifier, create a paged version iterator

  This implementation is inefficient if many of the qualifiers will have much less than a full page
  of data, because we will be doing a separate page request for each of them no matter what.  In C*
  Kiji, we could implement the map-family pager instead by issues one big query across all of the
  qualifiers.

- Milo mentioned three areas in which performance is important: bulk import, modeling training, and
  score function latency.

- If a user specifies that the maximum number of versions for a given locality group is only one,
  and if we choose to have one C* table per locality group, we can use a _slightly_ different layout
  for the C* table for that locality group



### Resource management

- Add code appropriately to keep track of refernces to `CassandraAdmin`, maybe to open `Session`s,
  etc.
- Implement the code to close open `Session`s when jobs are done.


### Other

- Double check that all rows with timestamps are ordered by DESC

- Check for any case-sensitivity issues - The CQL commands that we are using to create and
  manipulate tables may need some kind of quotes or other escaping to maintain upper/lower case.
    - Also really tighten up all of the places where we create queries and where we get actual C*
      table and keyspace names (want to pass around URIs and not strings).

- Do we need to make an interface for MetadataRestorer and then create HBase and C* versions?
  - Could have a static "get" method in KijiMetaTable that can get the appropriate restorer

- Figure out what to do about hashing entity IDs.  Right now we are really hashing twice
  (once in Kiji, once in Cassandra).  Do we want to rely on Kiji to hash entity IDs, and use a
  simple order-preserving hash in Cassandra?  That would allow us to implement row scans.

- `CassandraKijiRowData` does not need to use all of its fetched data to populate a map in its
  `getMap` method.  It should look at the data request and populate only those values that are valid
  with regard to requested columns, filters, max versions, etc.


Open questions
==============

These are meant to be higher-level issues than those in the TODO section above.

(This is kind of a random list.  There are other open items on the spreadsheet in google docs.)

### Table layout and data modeling

- Should we make a new `TableLayoutDesc` for C*-backed Kiji tables?
  - The current `TableLayoutDesc` has a lot of HBase-specific stuff in it.
  - A C*-specific layout description could allow users to indicate their most important query
    patterns, for example, which might allow us to change how we map locality groups, column
    families, versions, etc. into C*.
  - Currently the CQL primary key has the order (key, family, qualifier, version, value).  To
    support different query patterns, we might want to have a different ordering.
- How shall we expose C*'s variable consistency levels to the user?
- Do we need to support row scans?
- How do we support values for `MAXVERSIONS` other than `INFINITY` and 1?
- We are currently hashing every entity ID twice.  Once in Kiji, and then again when that entity ID
  becomes the C* partition key and it goes through the `Murmur3Partioner`.  Do we want to change
  this?
- Do we want allow users to encode some data using Cassandra's built-in types instead of always
  using Avro types?
  - Unless a user is doing some kind of nesting of lists, maps, or sets, he can do all of his data
    modeling now directly in the CQL data types.
- The [IntraVert](https://github.com/zznate/intravert-ug) project provides some server-side
  processing on top of Cassandra.  We may want to look into this project more carefully to see what
  whether we can leverage it for implementing filters, etc.

### Bulk importing

- Is there a C* equivalent of an HFile?  Most C* documentation indicates that we should be able to
  load a table often by just writing to it directly.
- DataStax has a blog post about an `sstableloader` tool.
- It looks like they added some [support](https://issues.apache.org/jira/browse/CASSANDRA-3045) for
  having a C* Hadoop job output files for bulk loading.
- The DataStax documentation for Cassandra 1.1
  [indicates](https://issues.apache.org/jira/browse/CASSANDRA-3045)
  that we can use something similar to HFiles called `BulkOutputFormat`.
- This [talk](http://www.slideshare.net/knewton/hadoop-and-cassandra-24950937) also looks useful.

### URIs

- Can we represent our Cassandra Kiji URIs more efficiently?  Is there any standard environment
  variable that contains a list of C* hosts and ports (so that a user could specify .env)?
- Do users need to be able to specify the Cassandra thrift port in the URI in addition to the native
  binary transport port?


Usage notes
===========

Cassandra 2.x requires JDK 7.  The following error indicates that you are using JDK 6:

    java.lang.UnsupportedClassVersionError: org/apache/cassandra/service/EmbeddedCassandraService : Unsupported major.minor version 51.0

To test with a Bento Box, I do the following:

- Symlink the KijiSchema JAR in the BentoBox lib directory to point to my local kiji-schema build
- I set my KIJI_CLASSPATH to be whatever Maven was using to build KijiSchema (yikes!) using `mvn
  dependency:build-classpath` to print out the classpath.

I cannot get the unit tests for this project to work within IntelliJ, so I have to run them all on
the command line.


Mapping Kiji to Cassandra
=========================

This section contains the current way that we map Kiji tables to Cassandra (we probably will modify
this in the future).


Kiji table
----------

There is a one-to-one mapping between Kiji tables and Cassandra tables.  Note that what is now a
"table" in Cassandra was previously a "column family."  We will refer to these tables/column
families as "tables" everywhere.

The one exception to the one-Cassandra-table-per-Kiji-table mapping is that we need to put counters
into their own tables.  Cassandra
[requires](http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_using/use_counter_t.html)
that counters go into special counter column tables.


Locality groups
---------------

**OPEN ITEM.**

We do not currently implement locality groups (other than using them as part of the namespace of the
Kiji table).  It would make sense to map Kiji locality groups into Cassandra tables, since
everything in a Cassandra table is located together, and Cassandra tables support all of the storage
options (caching, compression, etc.) that Kiji locality groups support.

Below are details about how we implement (or plan to implement) various features of locality groups:

#### Whether to store locality group information in memory or on disk

In a talk from the C* Summit EU 2013, one of the speakers
[describes](http://www.slideshare.net/planetcassandra/c-summit-eu-2013-playlists-at-spotify-using-cassandra-to-store-version-controlled-objects)
how to ensure that Cassandra keeps some tables in memory (see slide 32/37).  The bottom line from
his talk was that Cassandra's row cache did not work.  Maybe this is fixed in Cassandra 2.x.

#### Data retention lifetime

In Kiji, we specify TTL at the locality-group level.  CQL allows users to specify a TTL when
inserting data into a table.  We can allow users of C* Kiji to specify TTL for locality groups as
they do now and propagate that information down to every insert we perform.  We could also support
more fine-grained TTL specifications (down to the column level).

#### Maximum number of versions to keep

**OPEN ITEM.**

Note that supporting a maximum version of one is trivial.  Below in the section on C* table layouts
we describe how, but in short we modify the primary key specification from:

    PRIMARY KEY (key, qualifier, time)

to just:

    PRIMARY KEY (key, qualifier)

This way, only the most-recently-inserted value for a column will exist in the table.

I am not sure of a good way to implement a more flexible maximum number of versions in Cassandra.
Perhaps we can keep a per-column counter that stores the total number of versions we have and
whenever it exceeds a certain threshold (e.g., double the number we are supposed to keep) we can do
a batch delete.

#### Type of compression

CQL allows you to specify `LZ4`, `Snappy`, and `DEFALTE.`  The DataStax CQL documentation has a lot
of details about the compression options.

#### Bloom filter settings

CQL allows you to set the `bloom_filter_fp_chance` property for a table to tune the chance of
getting a false positive from your bloom filter.


Column families
---------------

We can implement map- and group-type families with Cassandra tables of this form:

    CREATE TABLE kiji_table_name_column_family_name (
      key blob,
      lg text,
      qualifier text,
      time long,
      value blob,
      PRIMARY KEY (key, qualifier, time)
    ) WITH CLUSTERING ORDER BY (qualifier ASC, time DESC);

If the max number of versions for a locality group is one, then we can modify the primary key
specification to remove `time`.  By removing the timestamp from the primary key, insertions for a
given row key and qualifier will always overwrite previous insertions, so we will have only one
version in the table at any time.  We could also leave `time` as a column in the table, but always
use the same value for `time` for every cell.

Note in this table structure, we can perform queries where we specify the key and qualifier and a
range for times, but we cannot perform queries where we specify the key and time and a range for
qualifiers.  If we want to support the latter type of queries, then we can create a second copy of
the table for the column family, but change the primary key order:

    PRIMARY KEY (key, time, qualifier)

The implementation shown above uses a straight `blob` as the entity Id, which means that we can
reuse all of the current Kiji code for entity IDs.  We could try to do something fancier, like
storing entity IDs somehow in Cassandra's partition keys, but this would require rewriting more of
the code (but it would make the databases more comprehensible with non-Kiji C* tools).


Counters
--------

Cassandra counters must go into their own tables.


CQL collections
---------------

CQL has nice built-in support for lists, maps, and sets.  I chose not to use them above, however,
for a couple of reasons:

- Any query that selects a collection returns the entire collection (no paging)
- The collections max out at 64K entries

As mentioned elsewhere, Cassandra's CQL collections (and other native schema types) would be most
useful for completely replacing our current Avro storage.


Other stuff we may want to expose
---------------------------------

- Compaction strategies
- Caching


Alternative implementation of Kiji in Cassandra
-----------------------------------------------

#### One C* table per Kiji locality group

This matches the Kiji HBase implementation more closely.  We could modify the table layout slightly
such that we add a column to the primary key to use for identifying a column family:

    CREATE TABLE kiji_table_name (
      key blob,
      family text,
      qualifier text,
      time long,
      value blob,
      PRIMARY KEY (key, qualifier, time)
    ) WITH CLUSTERING ORDER BY (family ASC, qualifier ASC, time DESC);

A locality group in Kiji is meant to contain data that is read together, as is a table in Cassanrda.

The downside of this implementation is that Kiji data requests that touch multiple locality groups
will require separate Cassandra RPC operations (we cannot put `SELECT` queries into a batch query).
Supporting such multi-RPC operations is possible, but pretty ugly, and would require changes in
KijiSchema, KijiMR, and in the Cassandra/Hadoop
[interface](https://github.com/wibiclint/cassandra2-hadoop2).


#### One Cassandra table per Kiji column family

We could have one Cassandra table per Kiji column family:

    CREATE TABLE kiji_table_name (
      key blob,
      qualifier text,
      time long,
      value blob,
      PRIMARY KEY (key, qualifier, time)
    ) WITH CLUSTERING ORDER BY (qualifier ASC, time DESC);

Such an implementation would allow finer-grained, per-Kiji-column-family filtering during requests,
since each Kiji column family (C* table) would get its own `SELECT` statement.  This finer-grained
server-side filtering might be worth the tradeoff in RPC calls under some circumstances.


Choosing when to return a Cassandra Kiji versus and HBase Kiji
==============================================================

We discussed several possible ways to decide whether to return a C* Kiji or an HBase Kiji (from a
Kiji factory, for example), including extending the current platform
[bridge](https://github.com/kijiproject/wiki/wiki/Platform-bridges) scheme.  Instead, we chose to
use different URIs for Cassandra- and HBase-backed Kiji instances.  These scheme allows a user to
support both Kiji platforms at the same time and to have them pass data back in forth through
KijiMR.

A current Kiji URI looks like:

    kiji://(zkHost,zkHost):zkPort/instance

Now, in addition to ZooKeeper hosts, we need to have to allow the user to specify Cassandra contact
points and a port (the port we pass around is the "native transport" port -- we do not allow the URI
to change the Cassandra thrift port).  For now, we'll use the following format:

    kiji-cassandra://(zkHost,zkHost):zkPort/(cassandraHost,cassandraHost)/cassandraPort/instance

We shall assume that URIs that don't explicitly start with `kiji-cassandra://` are HBase URIs.


Unit Testing
============

For unit tests, we use Cassandra's `EmbeddedCassandraService.`  Much as the HBase implementation of
Kiji has a `TestingHBaseFactory`, we have a `TestingCassandraFactory.`  This factory returns a
`CassandraAdmin` object that will contain a `Session` connected to our `EmbeddedCassandraService`.

The configuration for the Cassandra instance used for unit testing sits in
`src/main/test/resources/cassandra.yaml`.  This file specifies non-default port numbers to avoid
conflicts with a locally-running Cassandra instance.

Note that Cassandra limits keyspaces to 48 characters (?!), so we have to truncate the names of the
Kiji instances used for tests.
([Apparently](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/config/Schema.java#L49)
this is related to path-size limits in Windows!  Awesome!)

For integration tests, Joe recommends using a Vagrant virtual machine to set up a C* server.

Note that you can run just the C* unit tests at the command line with the command:

    mvn  -Dtest='*Cassandra*' test 


Paging
======

The Java drivers for Cassandra already support paging.  There is a little bit more information in
[this blog post](http://www.datastax.com/dev/blog/client-side-improvements-in-cassandra-2-0) about
paging in Cassandra 2.0.

Implementing paging for C* Kiji was straightforward.  The code is actually simpler than it was in
HBase in a lot of ways because the Java driver does so much of the heavy lifting.  The Kiji pagers
operate on a single column at a time, we didn't run into any of the difficulties that we normally
encounter (e.g., with regard to fetching data from lots of different columns at once, each column
having different filters, max versions, etc.).


Counters
========

Cassandra has limited support for
[counters:](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_counter_t.html)

- There is a separate CQL data type for counters.
- Counters must live in dedicated counter tables.
- A user cannot set a counter value - he/she can only increment a counter.
- If a user deletes a counter, the counter can never be used again (it will always be stuck at
  `null` if the user tries to increment it again).

This is different from HBase, in which (AFAIK) any column can be treated as a counter (just be
atomically interpreting its value as a long, incrementing it, and storing the results with the
current timestamp).  To support counters as best as we can in Cassandra Kiji, therefore, we do the
following:

- For every Kiji table, we create a second backing C* table (in addition to the one described
  earlier).  This table stores counter values and has the same layout as the original backing C*
  table, except that the values are of type `counter` instead of `blob`.

- Users can read counter values, but only if they do not specify a timestamp (which, according to
  the Kiji API, means that they are reading the most-recent value of the counter).

- Users can delete a counter, but if they do so, they will never be able to do anything useful with
  the counter again.  Subsequent writes and deletes will not error out, but will not do anything.

- Users can write counter values with a `KijiTableWriter`, but only if they do not specify a
  timestamp (which, according to the Kiji API, means that they are writing with the current
  timestamp).  We implement the write by reading back the current value of the counter and then
  incrementing the counter by the difference between the current value and the desired new value.

- Users cannot write a counter with a `KijiBufferedWriter` or with an `AtomicKijiPutter`, since both
  of those perform atomic sets of writes (and therefore cannot support our read-and-increment
  operation).

- Users can delete counters (or columns, families, or rows containing counters) in a
  `KijiTableWriter` or a `KijiBufferedWriter`.

Some traffic on the Cassandra users' mailing list indicates that the current (2.0.4) version of
Cassandra has problems with counters "drifting."  This should be fixed by version 2.1.


Filters
=======

The biggest open question in Cassandra Kiji is how to implement filters.  We have a range of options
open to us, between two extremes:

1. Fetch all of the data for an entire row (`SELECT * FROM mytable WHERE key=mykey`) and do all
filtering on the client side, in `CassandraKijiRowData`.

2. Fetch the data for a given `KijiDataRequest` in a large number of fine-grained queries, likely
one per qualified column, that will allow us to make greater us of the limited client-side filtering
options available in CQL (e.g., inequalities on columns and using `LIMIT`).

Several risks that I see:

- A user may design a query such that he expects the fetched data to _just_ fit into memory.  If,
  however, we have to use client-side filtering and we therefore fetch more data than the user has
  specified (and therefore more than he expects), the user may run out of memory.  To avoid this
  problem, we need to enable Cassandra paging in our requests to the Cassandra cluster, but figuring
  out when to do this, what setting to use for the paging, etc. is almost impossible.

- For some queries, avoiding massive amounts of client-side filtering may be impossible.
  Considering, for example, the case in which a user has a map-type family with 1M qualifiers, each
  of which has 1K versions.  If the users requests the most-recent version (the default setting for
  a `KijiDataRequest` of every column in the map-type family, we will have not choice but to fetch
  all of the data for all of the columns and filter on the client side.
