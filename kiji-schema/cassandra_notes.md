Notes about Cassandra development, refactoring, etc.
====================================================

This document includes notes about the C* versions of KijiSchema and KijiMR (almost all of the code
to support C* in Kiji is in KijiSchema).  I moved the super-detailed notes of everything that I did
every week into `cassandra_weekly_notes.md`.

Open TODOs
==========

### Major missing features

- Support paging!
- Add support for counters (CQL requires counters to be in separate tables, annoying...)
- Security / permission checking is not implemented at all now.
- Add support for filters (even if everything has to happen on the client for now).
- We need a C* version of `KijiTableAnnotator`.
- `CassandraKijiTableReader` has a few missing functions that should be easy to add (e.g.,
  `bulkGet`).
- We need a C* version of `AtomicKijiPutter`.
- The various table readers and writers are still missing some functionality (e.g., different
  flavors of deletes).  These shouldn't be too hard to add.

### General cleanup:

- Update copyrights, check for any stale HBase comments
- Add super-unstable annotations to this API.  :)

### Prepared CQL statements:

- Update any class that calls `Session#execute` to use a prepared query.

### Testing and error messages

- Need comprehensible error message if the user does not have Cassandra running (need an error
  message that is better than a stack trace)
- Desperately need more unit tests for the Cassandra / Hadoop code that I wrote, also for the
  record reader and input format in Kiji MR
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
- Need unit tests for changing table layouts

### General code organization

- Clean up / expand `KijiManagedCassandraTableName`
  - This is a total mess right now, with a mixture of static and non-static methods.
  - Make the methods in `KijiManagedCassandraTableName` more explicit about whether they are
    returning names in the Kiji namespace or in the C* namespace.
- Think about refactoring some code that is shared between HBase and Cassandra implementations of
  some components into new abstract superclasses or elsewhere.
  - There is lots of copy-paste code now, not good!
- Think about limiting the number of places from which we can call `Session#execute`.
  - Might be good to put all of these calls within `CassandraAdmin`, for example.
  - That would make it easier to manage prepared queries, since the queries are prepared
    per-`Session` (double-check that this is true) and the `CassandraAdmin` manages the active
    Cassandra `Session`.
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

### Performance

- Add some performance tests!
  - How does this perform versus HBase?
  - How does this perform versus bare-bones Cassandra?
- The `CassandraKijiBufferedWriter` should be aware of the replica nodes for different key ranges
  and send requests directly to the replica nodes.

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
  (once in Kiji, once in Cassandra).


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

We do not currently implement locality groups.  It would make sense to map Kiji locality groups into
Cassandra tables, since everything in a Cassandra table is located together, and Cassandra tables
support all of the storage options (caching, compression, etc.) that Kiji locality groups support.

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
paging in Cassandra 2.0.  Our challenge will be figuring out how to map the Cassandra paging
mechanism into the Kiji paging interface.


