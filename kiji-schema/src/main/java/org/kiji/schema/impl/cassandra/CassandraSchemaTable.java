/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.MD5Hash;
import org.kiji.schema.avro.SchemaTableBackup;
import org.kiji.schema.avro.SchemaTableEntry;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.kiji.schema.util.*;
import org.kiji.schema.util.ByteStreamArray.EncodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.kiji.schema.util.ByteStreamArray.longToVarInt64;

/**
 * <p>
 * Mapping between schema IDs, hashes and Avro schema objects.
 * This class is thread-safe.
 * </p>
 *
 * <p>
 * Schemas are stored in two tables with a single column family named "schema" and that contains
 * SchemaTableEntry records. One table is indexed by schema hashes (128-bit MD5 hashes of the
 * schema JSON representation). Other table is indexed by schema IDs (integers &gt;= 0).
 * There is a third table with a counter for the Schema IDs.
 *
 * There may be multiple schema IDs for a single schema.
 * </p>
 */
@ApiAudience.Private
public class CassandraSchemaTable implements KijiSchemaTable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSchemaTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + CassandraSchemaTable.class.getName());

  /** The column name in C* for the keys in the schema hash table. */
  public static final String SCHEMA_COLUMN_HASH_KEY = "schema_hash";

  /** The column name in C* for the keys in the schema ID table. */
  public static final String SCHEMA_COLUMN_ID_KEY = "schema_id";

  /** The column name in C* for the values in the schema hash and ID tables (same for both). */
  public static final String SCHEMA_COLUMN_VALUE = "schema_blob";

  /** The column name for the timestamp value in the schema hash and ID tables (same for both). */
  public static final String SCHEMA_COLUMN_TIME = "time";

  /** We need some kind of PRIMARY KEY column for the counter table. */
  public static final String SCHEMA_COUNTER_COLUMN_KEY = "counter_key";

  /** We should have only one row ever in this table... */
  public static final String SCHEMA_COUNTER_ONLY_KEY_VALUE = "THE_ONLY_COUNTER";

  /** The column name of the C* counter used to store schema IDs.  In C*, counters go into their own tables. */
  public static final String SCHEMA_COUNTER_COLUMN_VALUE = "counter";

  /** C* table used to map schema hash to schema entries. */
  private final CassandraTableInterface mSchemaHashTable;

  /** C* table used to map schema IDs to schema entries. */
  private final CassandraTableInterface mSchemaIdTable;

  /** Lock for the kiji instance schema table. */
  private final Lock mZKLock;

  /** C* table used to increment schema IDs. */
  private final CassandraTableInterface mCounterTable;

  /** Maps schema MD5 hashes to schema entries. */
  private final Map<BytesKey, SchemaEntry> mSchemaHashMap = new HashMap<BytesKey, SchemaEntry>();

  /** Maps schema IDs to schema entries. */
  private final Map<Long, SchemaEntry> mSchemaIdMap = new HashMap<Long, SchemaEntry>();

  /** Schema hash cache. */
  private final SchemaHashCache mHashCache = new SchemaHashCache();

  /** KijiURI of the Kiji instance this schema table belongs to. */
  private final KijiURI mURI;

  /** States of a SchemaTable instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this SchemaTable instance. */
  private AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Used for testing finalize() behavior. */
  private String mConstructorStack = "";

  /**
   * Creates a lock for a given Kiji instance.
   *
   * @param kijiURI URI of the Kiji instance.
   * @param factory Factory for locks.
   * @return a lock for the specified Kiji instance.
   * @throws IOException on I/O error.
   */
  public static Lock newLock(KijiURI kijiURI, LockFactory factory) throws IOException {
    final String name = new File("/kiji", kijiURI.getInstance()).toString();
    return factory.create(name);
  }

  /** Avro decoder factory. */
  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

  /** Avro encoder factory. */
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

  /** Avro reader for a schema entry. */
  private static final DatumReader<SchemaTableEntry> SCHEMA_ENTRY_READER =
      new SpecificDatumReader<SchemaTableEntry>(SchemaTableEntry.SCHEMA$);

  /** Avro writer for a schema entry. */
  private static final DatumWriter<SchemaTableEntry> SCHEMA_ENTRY_WRITER =
      new SpecificDatumWriter<SchemaTableEntry>(SchemaTableEntry.SCHEMA$);

  /** {@inheritDoc} */
  @Override
  public BytesKey getSchemaHash(Schema schema) {
    return mHashCache.getHash(schema);
  }

  /**
   * Decodes a binary-encoded Avro schema entry.
   *
   * @param bytes Binary-encoded Avro schema entry.
   * @return Decoded Avro schema entry.
   * @throws java.io.IOException on I/O error.
   */
  public static SchemaTableEntry decodeSchemaEntry(final byte[] bytes) throws IOException {
    final SchemaTableEntry entry = new SchemaTableEntry();
    final Decoder decoder =
        DECODER_FACTORY.directBinaryDecoder(new ByteArrayInputStream(bytes), null);
    return SCHEMA_ENTRY_READER.read(entry, decoder);
  }

  /**
   * Encodes an Avro schema entry into binary.
   *
   * @param avroEntry Avro schema entry to encode.
   * @return Binary-encoded Avro schema entry.
   * @throws java.io.IOException on I/O error.
   */
  public static byte[] encodeSchemaEntry(final SchemaTableEntry avroEntry) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream(4096);
    final Encoder encoder = ENCODER_FACTORY.directBinaryEncoder(bytes, null);
    SCHEMA_ENTRY_WRITER.write(avroEntry, encoder);
    return bytes.toByteArray();
  }

  /**
   * Creates an C* table handle to the schema hash table.
   *
   * @param kijiURI the KijiURI.
   * @param conf the Hadoop configuration.
   * @param admin Wrapper around C* session.
   * @return a new interface for the table storing the mapping from schema hash to schema entry.
   * @throws IOException on I/O error.
   */
  public static CassandraTableInterface newSchemaHashTable(
      KijiURI kijiURI,
      Configuration conf,
      CassandraAdmin admin)
      throws IOException {
    return admin.getCassandraTableInterface(
        KijiManagedCassandraTableName.getSchemaHashTableName(kijiURI.getInstance()).toString());
  }

  /**
   * Creates an C* table handle to the schema ID table.
   *
   * @param kijiURI the KijiURI.
   * @param conf the Hadoop configuration.
   * @param admin Wrapper around C* session.
   * @return a new interface for the table storing the mapping from schema ID to schema entry.
   * @throws IOException on I/O error.
   */
  public static CassandraTableInterface newSchemaIdTable(
      KijiURI kijiURI,
      Configuration conf,
      CassandraAdmin admin)
      throws IOException {
    return admin.getCassandraTableInterface(
        KijiManagedCassandraTableName.getSchemaIdTableName(kijiURI.getInstance()).toString());
  }

  /**
   * Creates an C* table handle to the schema counter table.
   *
   * @param kijiURI the KijiURI.
   * @param conf the Hadoop configuration.
   * @param admin Wrapper around C* session.
   * @return a new interface for the table storing the schema ID counter.
   * @throws IOException on I/O error.
   */
  public static CassandraTableInterface newSchemaCounterTable(
      KijiURI kijiURI,
      Configuration conf,
      CassandraAdmin admin)
      throws IOException {
    return admin.getCassandraTableInterface(
        KijiManagedCassandraTableName.getSchemaCounterTableName(kijiURI.getInstance()).toString());
  }

  /**
   * Wrap existing C* tables with schema mappings in them.
   * @param kijiURI
   * @param conf
   * @param admin
   * @param lockFactory
   * @throws IOException
   */
  public CassandraSchemaTable(
      KijiURI kijiURI,
      Configuration conf,
      CassandraAdmin admin,
      LockFactory lockFactory)
      throws IOException {
    this(newSchemaHashTable(kijiURI, conf, admin),
        newSchemaIdTable(kijiURI, conf, admin),
        newSchemaCounterTable(kijiURI, conf, admin),
        newLock(kijiURI, lockFactory),
        kijiURI);
  }


  /**
   * Wrap an existing HBase table assumed to be where the schema data is stored.
   *
   * @param hashTable The HTable that maps schema hashes to schema entries.
   * @param idTable The HTable that maps schema IDs to schema entries.
   * @param uri URI of the Kiji instance this schema table belongs to.
   * @throws java.io.IOException on I/O error.
   */
  // TODO: Check whether this needs to be public
  private CassandraSchemaTable(
      CassandraTableInterface hashTable,
      CassandraTableInterface idTable,
      CassandraTableInterface counterTable,
      Lock zkLock,
      KijiURI uri)
      throws IOException {
    mSchemaHashTable = Preconditions.checkNotNull(hashTable);
    mSchemaIdTable = Preconditions.checkNotNull(idTable);
    mCounterTable = Preconditions.checkNotNull(counterTable);
    mZKLock = Preconditions.checkNotNull(zkLock);
    mURI = uri;

    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open SchemaTable instance in state %s.", oldState);
  }

  // TODO: Probably add a constructor that gets a CassandraAdmin and uses that to create the m*Tables

  /**
   * Looks up a schema entry given an Avro schema object.
   *
   * Looks first in-memory. If the schema is not known in-memory, looks in the HTables.
   *
   * @param schema Avro schema to look up.
   * @return Either the pre-existing entry for the specified schema, or a newly created entry.
   * @throws java.io.IOException on I/O error.
   */
  private synchronized SchemaEntry getOrCreateSchemaEntry(final Schema schema) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get or create schema entry from SchemaTable instance in state %s.", state);

    final BytesKey schemaHash = getSchemaHash(schema);
    final SchemaEntry knownEntry = getSchemaEntry(schemaHash);
    if (knownEntry != null) {
      return knownEntry;
    }

    // Schema is unknown, both in-memory and in-table.
    // Allocate a new schema ID and write it down to the tables:
    return storeInMemory(registerNewSchemaInTable(schema, schemaHash));
  }

  /** {@inheritDoc} */
  @Override
  public long getOrCreateSchemaId(final Schema schema) throws IOException {
    return getOrCreateSchemaEntry(schema).getId();
  }

  /** {@inheritDoc} */
  @Override
  public BytesKey getOrCreateSchemaHash(final Schema schema) throws IOException {
    return getOrCreateSchemaEntry(schema).getHash();
  }

  /**
   * Registers a new schema into the schema tables.
   *
   * The following things happen atomically, while holding a lock on the counter row:
   *   <li> look up the schema from the hash table, returning the entry if it is found; </li>
   *   <li> allocate a new unique ID for the schema (by incrementing the schema counter); </li>
   *   <li> write the new schema entry to the hash table and the ID table. </li>
   *
   * @param schema Avro schema to register
   * @param schemaHash hash of the schema
   * @return Fully populated SchemaEntry
   * @throws java.io.IOException on I/O error.
   */
  private SchemaEntry registerNewSchemaInTable(final Schema schema, final BytesKey schemaHash)
      throws IOException {
    mZKLock.lock();
    try {
      final SchemaTableEntry existingAvroEntry = loadFromHashTable(schemaHash);
      if (existingAvroEntry != null) {
        return fromAvroEntry(existingAvroEntry);
      }

      // Here we know the schema is unknown from the schema tables and no other process can
      // update the schema table.
      incrementSchemaIdCounter(1);
      long schemaId = readSchemaIdCounter();

      final SchemaEntry entry = new SchemaEntry(schemaId, schemaHash, schema);
      storeInTable(toAvroEntry(entry));
      return entry;

    } finally {
      mZKLock.unlock();
    }
  }

  /**
   * Increment the schema ID counter.
   * @param incrementAmount Amount by which to increment the counter (can be negative).
   */
  private void incrementSchemaIdCounter(long incrementAmount) {
    String tableName = mCounterTable.getTableName();
    Session session = mCounterTable.getSession();
    String incrementSign = incrementAmount >= 0 ? "+" : "-";
    String queryText = String.format("UPDATE %s SET %s = %s %s %d WHERE %s='%s';",
        tableName,
        SCHEMA_COUNTER_COLUMN_VALUE,
        SCHEMA_COUNTER_COLUMN_VALUE,
        incrementSign,
        incrementAmount,
        SCHEMA_COUNTER_COLUMN_KEY,
        SCHEMA_COUNTER_ONLY_KEY_VALUE
    );
    session.execute(queryText);
  }

  /**
   * Read back the current value of the schema ID counter.
   * @return Value of the counter.
   */
  private long readSchemaIdCounter() {
    // Sanity check that counter value is 1!
    String queryText = String.format("SELECT * FROM %s;", mCounterTable.getTableName());
    ResultSet resultSet = mCounterTable.getSession().execute(queryText);
    List<Row> rows = resultSet.all();
    assert(rows.size() == 1);
    Row row = rows.get(0);
    return row.getLong(SCHEMA_COUNTER_COLUMN_VALUE);
  }

  /**
   * Used for resetting the schema ID counter.
   *
   * This is fairly hackish and relies upon the counter being locked with a ZooKeeper lock.
   * @param newCounterValue Value to which to set the counter.
   */
  private void setSchemaIdCounter(long newCounterValue) {
    // Get the current counter value
    long currentValue = readSchemaIdCounter();
    incrementSchemaIdCounter(newCounterValue - currentValue);
  }

  /**
   * Writes the given schema entry to the ID and hash tables.
   *
   * This is not protected from concurrent writes. Caller must ensure consistency.
   *
   * @param avroEntry Schema entry to write.
   * @throws java.io.IOException on I/O error.
   */
  private void storeInTable(final SchemaTableEntry avroEntry)
      throws IOException {
    storeInTable(avroEntry, HConstants.LATEST_TIMESTAMP, true);
  }

  /**
   * Writes the given schema entry to the ID and hash tables.
   *
   * This is not protected from concurrent writes. Caller must ensure consistency.
   *
   * @param avroEntry Schema entry to write.
   * @param timestamp Write entries with this timestamp.
   * @param flush Whether to flush tables synchronously.
   * @throws java.io.IOException on I/O error.
   */
  private void storeInTable(final SchemaTableEntry avroEntry, long timestamp, boolean flush)
      throws IOException {
    // TODO: Replace with actual C* code
    final byte[] entryBytes = encodeSchemaEntry(avroEntry);

    // TODO: Obviate this comment by doing all of this in batch.
    // Writes the ID mapping first: if the hash table write fails, we just lost one schema ID.
    // The hash table write must not happen before the ID table write has been persisted.
    // Otherwise, another client may see the hash entry, write cells with the schema ID that cannot
    // be decoded (since the ID mapping has not been written yet).
    Session idSession = mSchemaIdTable.getSession();

    String idQueryText = String.format(
        "INSERT INTO %s(%s, %s, %s) VALUES(?, ?, ?);",
        mSchemaIdTable.getTableName(),
        SCHEMA_COLUMN_ID_KEY,
        SCHEMA_COLUMN_TIME,
        SCHEMA_COLUMN_VALUE);

    PreparedStatement idPreparedStatement = idSession.prepare(idQueryText);
    ResultSet resultSet = idSession.execute(idPreparedStatement.bind(
        avroEntry.getId(),
        new Date(timestamp),
        CassandraByteUtil.bytesToByteBuffer(entryBytes)));

    // TODO: Anything here to flush the table or verify that this worked?
    //if (flush) { mSchemaIdTable.flushCommits(); }

    Session hashSession = mSchemaHashTable.getSession();

    String hashQueryText = String.format(
        "INSERT INTO %s(%s, %s, %s) VALUES(?, ?, ?);",
        mSchemaHashTable.getTableName(),
        SCHEMA_COLUMN_HASH_KEY,
        SCHEMA_COLUMN_TIME,
        SCHEMA_COLUMN_VALUE);

    PreparedStatement hashPreparedStatement = hashSession.prepare(hashQueryText);
    ResultSet hashResultSet = hashSession.execute(hashPreparedStatement.bind(
        CassandraByteUtil.bytesToByteBuffer(avroEntry.getHash().bytes()),
        new Date(timestamp),
        CassandraByteUtil.bytesToByteBuffer(entryBytes)));

    // TODO: Anything here to flush the table or verify that this worked?
    //if (flush) { mSchemaHashTable.flushCommits(); }
  }

  /**
   * Fetches a schema entry from the tables given a schema ID.
   *
   * @param schemaId schema ID
   * @return Avro schema entry, or null if the schema ID does not exist in the table
   * @throws java.io.IOException on I/O error.
   */
  private SchemaTableEntry loadFromIdTable(long schemaId) throws IOException {
    Session session = mSchemaIdTable.getSession();
    String tableName = mSchemaIdTable.getTableName();

    // TODO: Prepare this statement once in constructor, not every load.
    String queryText = String.format(
        "SELECT %s FROM %s WHERE %s=%d ORDER BY %s DESC LIMIT 1",
        SCHEMA_COLUMN_VALUE,
        tableName,
        SCHEMA_COLUMN_ID_KEY,
        schemaId,
        SCHEMA_COLUMN_TIME
    );
    ResultSet resultSet = session.execute(queryText);
    List<Row> rows = resultSet.all();

    if (0 == rows.size()) {
      return null;
    }

    assert(rows.size() == 1);
    byte[] schemaAsBytes = CassandraByteUtil.byteBuffertoBytes(rows.get(0).getBytes(SCHEMA_COLUMN_VALUE));
    return decodeSchemaEntry(schemaAsBytes);
  }

  /**
   * Fetches a schema entry from the tables given a schema hash.
   *
   * @param schemaHash schema hash
   * @return Avro schema entry, or null if the schema hash does not exist in the table
   * @throws java.io.IOException on I/O error.
   */
  private SchemaTableEntry loadFromHashTable(BytesKey schemaHash) throws IOException {
    Session session = mSchemaHashTable.getSession();
    String tableName = mSchemaHashTable.getTableName();

    ByteBuffer tableKey = CassandraByteUtil.bytesToByteBuffer(schemaHash.getBytes());

    // TODO: Prepare this statement once in constructor, not every load.
    String queryText = String.format(
        "SELECT %s FROM %s WHERE %s=? ORDER BY %s DESC LIMIT 1",
        SCHEMA_COLUMN_VALUE,
        tableName,
        SCHEMA_COLUMN_HASH_KEY,
        SCHEMA_COLUMN_TIME
    );
    PreparedStatement preparedStatement = session.prepare(queryText);
    ResultSet resultSet = session.execute(preparedStatement.bind(tableKey));
    List<Row> rows = resultSet.all();

    if (0 == rows.size()) {
      return null;
    }

    assert(rows.size() == 1);
    byte[] schemaAsBytes = CassandraByteUtil.byteBuffertoBytes(rows.get(0).getBytes(SCHEMA_COLUMN_VALUE));
    return decodeSchemaEntry(schemaAsBytes);
  }

  /**
   * Converts an Avro SchemaTableEntry into a SchemaEntry.
   *
   * @param avroEntry Avro SchemaTableEntry
   * @return an equivalent SchemaEntry
   */
  public static SchemaEntry fromAvroEntry(final SchemaTableEntry avroEntry) {
    final String schemaJson = avroEntry.getAvroSchema();
    final Schema schema = new Schema.Parser().parse(schemaJson);
    return new SchemaEntry(avroEntry.getId(), new BytesKey(avroEntry.getHash().bytes()), schema);
  }

  /**
   * Converts a SchemaEntry into an Avro SchemaTableEntry.
   *
   * @param entry a SchemaEntry.
   * @return an equivalent Avro SchemaTableEntry.
   */
  public static SchemaTableEntry toAvroEntry(final SchemaEntry entry) {
    return SchemaTableEntry.newBuilder().setId(entry.getId())
        .setHash(new MD5Hash(entry.getHash().getBytes()))
        .setAvroSchema(entry.getSchema().toString()).build();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized Schema getSchema(long schemaId) throws IOException {
    final SchemaEntry entry = getSchemaEntry(schemaId);
    return (entry == null) ? null : entry.getSchema();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized SchemaEntry getSchemaEntry(long schemaId) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get schema entry from SchemaTable instance in state %s.", state);

    final SchemaEntry existingEntry = mSchemaIdMap.get(schemaId);
    if (existingEntry != null) {
      return existingEntry;
    }

    // On a lookup miss from the local schema cache, check to see if we can get the schema
    // from the original HBase table, cache it locally, and return it.
    final SchemaTableEntry avroEntry = loadFromIdTable(schemaId);
    if (avroEntry == null) {
      return null;
    }
    return storeInMemory(avroEntry);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getSchema(BytesKey schemaHash) throws IOException {
    final SchemaEntry entry = getSchemaEntry(schemaHash);
    return (entry == null) ? null : entry.getSchema();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized SchemaEntry getSchemaEntry(BytesKey schemaHash) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get schema entry from SchemaTable instance in state %s.", state);

    final SchemaEntry existingEntry = mSchemaHashMap.get(schemaHash);
    if (existingEntry != null) {
      return existingEntry;
    }

    // On a lookup miss from the local schema cache, check to see if we can get the schema
    // from the original HBase table, cache it locally, and return it.
    final SchemaTableEntry avroEntry = loadFromHashTable(schemaHash);
    if (null == avroEntry) {
      return null;
    }
    final SchemaEntry entry = storeInMemory(avroEntry);
    Preconditions.checkState(schemaHash.equals(entry.getHash()));
    return entry;
  }

  /** {@inheritDoc} */
  @Override
  public SchemaEntry getSchemaEntry(Schema schema) throws IOException {
    return getSchemaEntry(getSchemaHash(schema));
  }

  /**
   * Stores the specified schema entry in memory.
   *
   * External synchronization required.
   *
   * @param avroEntry Avro schema entry.
   * @return the SchemaEntry stored in memory.
   */
  private SchemaEntry storeInMemory(final SchemaTableEntry avroEntry) {
    return storeInMemory(fromAvroEntry(avroEntry));
  }

  /**
   * Stores the specified schema entry in memory.
   *
   * External synchronization required.
   *
   * @param entry the SchemaEntry to store in memory.
   * @return the SchemaEntry stored in memory.
   */
  private SchemaEntry storeInMemory(final SchemaEntry entry) {
    // Replacing an hash-mapped entry may happen, if two different IDs were assigned to one schema.
    final SchemaEntry oldHashEntry = mSchemaHashMap.put(entry.getHash(), entry);
    if (oldHashEntry != null) {
      LOG.info(String.format(
          "Replacing hash-mapped schema entry:%n%s%nwith:%n%s", oldHashEntry, entry));
    }

    // Replacing an ID-mapped entry should never happen:
    // IDs are associated to at most one schema/hash.
    final SchemaEntry oldIdEntry = mSchemaIdMap.put(entry.getId(), entry);
    if (oldIdEntry != null) {
      throw new AssertionError(String.format(
          "Attempting to replace ID-mapped schema entry:%n%s%nwith:%n%s", oldIdEntry, entry));
    }
    return entry;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void flush() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot flush SchemaTable instance in state %s.", state);
    // TODO: Replace with actual C* code
    //mSchemaIdTable.flushCommits();
    //mSchemaHashTable.flushCommits();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    flush();
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close SchemaTable instance in state %s.", oldState);
    // TODO: Replace with actual C* code
    //mSchemaHashTable.close();
    //mSchemaIdTable.close();
    ResourceUtils.closeOrLog(mZKLock);
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      CLEANUP_LOG.warn("Finalizing unclosed SchemaTable instance %s in state %s.", this, state);
      CLEANUP_LOG.debug("Stack when HBaseSchemaTable was constructed:\n" + mConstructorStack);
      close();
    }
    super.finalize();
  }


  /**
   * Install the schema hash table.
   * @param admin
   * @param tableName
   */
  private static CassandraTableInterface installHashTable(CassandraAdmin admin, String tableName) {
    // Let's try to make this somewhat readable...
    // TODO: Table should order by DESC for time
    String tableDescription = String.format(
        "(%s blob, %s timestamp, %s blob, PRIMARY KEY (%s, %s));",
        SCHEMA_COLUMN_HASH_KEY,
        SCHEMA_COLUMN_TIME,
        SCHEMA_COLUMN_VALUE,
        SCHEMA_COLUMN_HASH_KEY,
        SCHEMA_COLUMN_TIME
    );
    return admin.createTable(tableName, tableDescription);
  }

  /**
   * Install the schema ID table.
   * @param admin
   * @param tableName
   */
  private static CassandraTableInterface installIdTable(CassandraAdmin admin, String tableName) {
    // TODO: Table should order by DESC for time
    String tableDescription = String.format(
        "(%s bigint, %s timestamp, %s blob, PRIMARY KEY (%s, %s));",
        SCHEMA_COLUMN_ID_KEY,
        SCHEMA_COLUMN_TIME,
        SCHEMA_COLUMN_VALUE,
        SCHEMA_COLUMN_ID_KEY,
        SCHEMA_COLUMN_TIME
    );
    return admin.createTable(tableName, tableDescription);
  }

  /**
   * Install the schema ID counter table.
   * @param admin
   * @param tableName
   */
  private static CassandraTableInterface installCounterTable(CassandraAdmin admin, String tableName) {
    String tableDescription = String.format(
        "(%s text PRIMARY KEY, %s counter);",
        SCHEMA_COUNTER_COLUMN_KEY,
        SCHEMA_COUNTER_COLUMN_VALUE
    );
    CassandraTableInterface ctable = admin.createTable(tableName, tableDescription);

    // Now set the counter to zero
    String queryText = String.format("UPDATE %s SET %s = %s + 0 WHERE %s='%s';",
        tableName,
        SCHEMA_COUNTER_COLUMN_VALUE,
        SCHEMA_COUNTER_COLUMN_VALUE,
        SCHEMA_COUNTER_COLUMN_KEY,
        SCHEMA_COUNTER_ONLY_KEY_VALUE
    );
    LOG.debug(queryText);
    admin.getSession().execute(queryText);

    // Sanity check that counter value is 1!
    queryText = String.format("SELECT * FROM %s;", tableName);
    ResultSet resultSet = admin.getSession().execute(queryText);
    List<Row> rows = resultSet.all();
    assert(rows.size() == 1);
    Row row = rows.get(0);
    long counterValue = row.getLong(SCHEMA_COUNTER_COLUMN_VALUE);
    assert(0 == counterValue);

    return ctable;
  }

  /**
   * Install the schema table into a Kiji instance.
   *
   * @param admin The C* Admin interface for the HBase cluster to install into.
   * @param kijiURI the KijiURI.
   * @param conf The Hadoop configuration.
   * @throws java.io.IOException on I/O error.
   */
  public static void install(
      CassandraAdmin admin,
      KijiURI kijiURI,
      Configuration conf,
      LockFactory lockFactory)
      throws IOException {
    // Keep all versions of schema entries:
    //  - entries of the ID table should never be written more than once.
    //  - entries of the hash table could be written more than once:
    //      - with different schema IDs in some rare cases, for example when a client crashes
    //        while writing an entry.
    //      - with different schemas on MD5 hash collisions.

    CassandraTableInterface hashTable = installHashTable(
        admin,
        KijiManagedCassandraTableName.getSchemaHashTableName(kijiURI.getInstance()).toString());

    CassandraTableInterface idTable = installIdTable(
        admin,
        KijiManagedCassandraTableName.getSchemaIdTableName(kijiURI.getInstance()).toString());

    CassandraTableInterface counterTable = installCounterTable(
        admin,
        KijiManagedCassandraTableName.getSchemaCounterTableName(kijiURI.getInstance()).toString());

    final CassandraSchemaTable schemaTable = new CassandraSchemaTable(
        hashTable,
        idTable,
        counterTable,
        newLock(kijiURI, lockFactory),
        kijiURI);
    try {
      schemaTable.registerPrimitiveSchemas();
    } finally {
      ResourceUtils.closeOrLog(schemaTable);
    }
  }

  /**
   * Deletes a C* table.
   *
   * @param admin C* admin client.
   * @param tableName Name of the table to delete.
   */
  private static void deleteTable(CassandraAdmin admin, String tableName) {
    // TODO: Replace with actual C* code
    /*
    try {
      if (admin.tableExists(tableName)) {
        if (admin.isTableEnabled(tableName)) {
          admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
      }
    } catch (IOException ioe) {
      LOG.error(String.format("Unable to delete table '%s': %s", tableName, ioe.toString()));
    }
    */
  }

  /**
   * Disables and removes the schema table from HBase.
   *
   * @param admin The HBase Admin object.
   * @param kijiURI The KijiURI for the instance to remove.
   * @throws java.io.IOException If there is an error.
   */
  public static void uninstall(CassandraAdmin admin, KijiURI kijiURI)
  // TODO: Replace with actual C* code
      throws IOException {
    /*
    final String hashTableName =
        KijiManagedHBaseTableName.getSchemaHashTableName(kijiURI.getInstance()).toString();
    deleteTable(admin, hashTableName);

    final String idTableName =
        KijiManagedHBaseTableName.getSchemaIdTableName(kijiURI.getInstance()).toString();
    deleteTable(admin, idTableName);
    */
  }

  /** {@inheritDoc} */
  @Override
  public SchemaTableBackup toBackup() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot backup SchemaTable instance in state %s.", state);
    // TODO: Figure out how to implement this lock in C*. (This is not as simple as using a batch operation most likely...)
    mZKLock.lock();
    List<SchemaTableEntry> entries = Lists.newArrayList();
    try {
      /** Entries from the schema hash table. */
      final Set<SchemaEntry> hashTableEntries = loadSchemaHashTable(mSchemaHashTable);
      if (!checkConsistency(hashTableEntries)) {
        LOG.error("Schema hash table is inconsistent");
      }

      /** Entries from the schema ID table. */
      final Set<SchemaEntry> idTableEntries = loadSchemaIdTable(mSchemaIdTable);
      if (!checkConsistency(idTableEntries)) {
        LOG.error("Schema hash table is inconsistent");
      }

      final Set<SchemaEntry> mergedEntries = new HashSet<SchemaEntry>(hashTableEntries);
      mergedEntries.addAll(idTableEntries);
      if (!checkConsistency(mergedEntries)) {
        LOG.error("Merged schema hash and ID tables are inconsistent");
      }
      for (SchemaEntry entry : mergedEntries) {
        entries.add(toAvroEntry(entry));
      }
    } finally {
      mZKLock.unlock();
    }
    return SchemaTableBackup.newBuilder().setEntries(entries).build();
  }

  /** {@inheritDoc} */
  @Override
  public void fromBackup(final SchemaTableBackup backup) throws IOException {
    // TODO: Replace with actual C* code
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot restore backup to SchemaTable instance in state %s.", state);
    // TODO: Figure out how to implement this lock in C*. (This is not as simple as using a batch operation most likely...)
    mZKLock.lock();
    try {
      /** Entries from the schema hash table. */
      final Set<SchemaEntry> hashTableEntries = loadSchemaHashTable(mSchemaHashTable);

      /** Entries from the schema ID table. */
      final Set<SchemaEntry> idTableEntries = loadSchemaIdTable(mSchemaIdTable);

      final Set<SchemaEntry> mergedEntries = new HashSet<SchemaEntry>(hashTableEntries);
      mergedEntries.addAll(idTableEntries);
      if (!checkConsistency(mergedEntries)) {
        LOG.error("Merged schema hash and ID tables are inconsistent");
      }

      final List<SchemaTableEntry> avroBackupEntries = backup.getEntries();
      final Set<SchemaEntry> schemaTableEntries =
          new HashSet<SchemaEntry>(avroBackupEntries.size());
      for (SchemaTableEntry avroEntry : avroBackupEntries) {
        schemaTableEntries.add(fromAvroEntry(avroEntry));
      }
      if (!checkConsistency(schemaTableEntries)) {
        LOG.error("Backup schema entries are inconsistent");
      }

      mergedEntries.addAll(schemaTableEntries);
      if (!checkConsistency(schemaTableEntries)) {
        LOG.error("Backup schema entries are inconsistent with already existing schema entries");
      }

      long maxSchemaId = -1L;
      for (SchemaEntry entry : mergedEntries) {
        maxSchemaId = Math.max(maxSchemaId, entry.getId());
      }
      final long nextSchemaId = maxSchemaId + 1;

      flush();
      //SchemaPlatformBridge.get().setWriteBufferSize(mSchemaIdTable, schemaTableEntries.size() + 1);
      //SchemaPlatformBridge.get().setWriteBufferSize(mSchemaHashTable, schemaTableEntries.size());

      // Restored schema entries share the same timestamp:
      final long timestamp = System.currentTimeMillis();
      for (SchemaEntry entry : schemaTableEntries) {
        storeInTable(toAvroEntry(entry), timestamp, false);  // do not flush
      }
      setSchemaIdCounter(nextSchemaId);
      flush();
    } finally {
      mZKLock.unlock();
    }
  }

  /**
   * Checks the consistency of a collection of schema entries.
   *
   * @param entries Collection of schema entries.
   * @return whether the entries are consistent.
   */
  private static boolean checkConsistency(Set<SchemaEntry> entries) {
    final Map<Long, SchemaEntry> idMap = new HashMap<Long, SchemaEntry>(entries.size());
    final Map<BytesKey, SchemaEntry> hashMap = new HashMap<BytesKey, SchemaEntry>(entries.size());
    boolean isConsistent = true;

    for (SchemaEntry entry : entries) {
      final SchemaEntry existingEntryWithId = idMap.put(entry.getId(), entry);
      if ((existingEntryWithId != null) && !existingEntryWithId.equals(entry)) {
        LOG.error(String.format("Conflicting schema entries with ID %d: %s vs %s",
            entry.getId(), entry, existingEntryWithId));
        isConsistent = false;
      }
      final SchemaEntry existingEntryWithHash = hashMap.put(entry.getHash(), entry);
      if ((existingEntryWithHash != null) && !existingEntryWithHash.equals(entry)) {
        if (existingEntryWithHash.getHash().equals(entry.getHash())
            && existingEntryWithHash.getSchema().equals(entry.getSchema())) {
          // Does not affect consistency:
          LOG.info(String.format("Schema with hash %s has multiple IDs: %d, %d: %s",
              entry.getHash(), entry.getId(), existingEntryWithHash.getId(), entry.getSchema()));
        } else {
          LOG.info(String.format("Conflicting schema entries with hash %s: %s vs %s",
              entry.getHash(), entry, existingEntryWithHash));
          isConsistent = false;
        }
      }
    }
    return isConsistent;
  }

  /** Primitive types pre-allocated in all schema tables. */
  enum PreRegisteredSchema {
    STRING(Schema.Type.STRING),   // ID 0
    BYTES(Schema.Type.BYTES),     // ID 1
    INT(Schema.Type.INT),         // ID 2
    LONG(Schema.Type.LONG),       // ID 3
    FLOAT(Schema.Type.FLOAT),     // ID 4
    DOUBLE(Schema.Type.DOUBLE),   // ID 5
    BOOLEAN(Schema.Type.BOOLEAN), // ID 6
    NULL(Schema.Type.NULL);       // ID 7

    /**
     * Initializes a pre-registered schema descriptor.
     *
     * @param type Avro schema type.
     */
    PreRegisteredSchema(Schema.Type type) {
      mType = type;
      mId = ordinal();
    }

    /** @return the Avro schema type. */
    public Schema.Type getType() {
      return mType;
    }

    /** @return the unique ID of the pre-allocated schema. */
    public int getSchemaId() {
      // By default, we use the enum ordinal
      return mId;
    }

    private final int mId;
    private final Schema.Type mType;
  }

  /** Number of pre-allocated schemas. */
  public static final int PRE_REGISTERED_SCHEMA_COUNT = PreRegisteredSchema.values().length;  // = 8

  /**
   * Pre-registers all the primitive data types.
   *
   * @throws java.io.IOException on I/O failure.
   */
  private synchronized void registerPrimitiveSchemas() throws IOException {
    int expectedSchemaId = 0;
    LOG.debug("Pre-registering primitive schema types.");
    for (PreRegisteredSchema desc : PreRegisteredSchema.values()) {
      final Schema schema = Schema.create(desc.getType());
      Preconditions.checkState(getOrCreateSchemaId(schema) == expectedSchemaId);
      Preconditions.checkState(desc.getSchemaId() == expectedSchemaId);
      expectedSchemaId += 1;
    }
    Preconditions.checkState(expectedSchemaId == PRE_REGISTERED_SCHEMA_COUNT);
  }

  /**
   * Loads and check the consistency of the schema hash table.
   *
   * @param hashTable schema hash HTable.
   * @return the set of schema entries from the schema hash table.
   * @throws java.io.IOException on I/O error.
   */
  private Set<SchemaEntry> loadSchemaHashTable(CassandraTableInterface hashTable) throws IOException {
    // TODO: Replace with actual C* code
    LOG.info("Loading entries from schema hash table.");
    final Set<SchemaEntry> entries = new HashSet<SchemaEntry>();
    int hashTableRowCounter = 0;

    // Fetch all of the schemas from the schema hash table (all versions)
    String queryText = String.format("SELECT * FROM %s;", hashTable.getTableName());
    ResultSet resultSet = hashTable.getSession().execute(queryText);

    for (Row row : resultSet) {
      hashTableRowCounter += 1;

      // TODO: Not sure how to replicate this check in C*...
      /*
      if (result.getRow().length != Hasher.HASH_SIZE_BYTES) {
        LOG.error(String.format(
            "Invalid schema hash table row key size: %s, expecting %d bytes.",
            new BytesKey(result.getRow()), Hasher.HASH_SIZE_BYTES));
        continue;
      */

      // Get the row key, timestamp, and schema for this row
      final BytesKey rowKey = new BytesKey(CassandraByteUtil.byteBuffertoBytes(row.getBytes(SCHEMA_COLUMN_HASH_KEY)));
      final long timestamp = row.getLong(SCHEMA_COLUMN_TIME);
      final byte[] schemaAsBytes = CassandraByteUtil.byteBuffertoBytes(row.getBytes(SCHEMA_COLUMN_VALUE));

      try {
        final SchemaEntry entry = fromAvroEntry(decodeSchemaEntry(schemaAsBytes));
        entries.add(entry);
        if (!getSchemaHash(entry.getSchema()).equals(entry.getHash())) {
          LOG.error(String.format(
              "Invalid schema hash table entry: computed schema hash %s does not match entry %s",
              getSchemaHash(entry.getSchema()), entry));
        }
        if (!rowKey.equals(entry.getHash())) {
          LOG.error(String.format("Inconsistent schema hash table: "
              + "hash encoded in row key %s does not match schema entry: %s",
              rowKey, entry));
        }
      } catch (IOException ioe) {
        LOG.error(String.format(
            "Unable to decode schema hash table entry for row %s, timestamp %d: %s",
            rowKey, timestamp, ioe));
        continue;
      } catch (AvroRuntimeException are) {
        LOG.error(String.format(
            "Unable to decode schema hash table entry for row %s, timestamp %d: %s",
            rowKey, timestamp, are));
        continue;
      }
    }
    LOG.info(String.format(
        "Schema hash table has %d rows and %d entries.", hashTableRowCounter, entries.size()));
    return entries;
  }

  /**
   * Loads and check the consistency of the schema ID table.
   *
   * @param idTable schema ID HTable.
   * @return the set of schema entries from the schema ID table.
   * @throws java.io.IOException on I/O error.
   */
  private Set<SchemaEntry> loadSchemaIdTable(CassandraTableInterface idTable) throws IOException {
    // TODO: Replace with actual C* code
    LOG.info("Loading entries from schema ID table.");
    int idTableRowCounter = 0;
    final Set<SchemaEntry> entries = new HashSet<SchemaEntry>();

    // Fetch all of the schemas from the schema ID table (all versions)
    String queryText = String.format("SELECT * FROM %s;", idTable.getTableName());
    ResultSet resultSet = idTable.getSession().execute(queryText);

    for (Row row : resultSet) {
      idTableRowCounter += 1;

      // Get the row key, timestamp, and schema for this row
      // Use "Unsafe" version of method here to get raw bytes no matter what format the field is in the C* table.
      final BytesKey rowKey = new BytesKey(CassandraByteUtil.byteBuffertoBytes(row.getBytesUnsafe(SCHEMA_COLUMN_ID_KEY)));

      long schemaId = -1;
      try {
        schemaId = row.getLong(SCHEMA_COLUMN_ID_KEY);
      } catch (InvalidTypeException exn) {
        LOG.error(String.format("Unable to decode schema ID encoded in row key %s: %s",
            rowKey, exn));
      }

      final long timestamp = row.getLong(SCHEMA_COLUMN_TIME);
      final byte[] schemaAsBytes = CassandraByteUtil.byteBuffertoBytes(row.getBytes(SCHEMA_COLUMN_VALUE));
      try {
        final SchemaEntry entry = fromAvroEntry(decodeSchemaEntry(schemaAsBytes));
        entries.add(entry);
        if (!getSchemaHash(entry.getSchema()).equals(entry.getHash())) {
          LOG.error(String.format("Invalid schema hash table entry with row key %s: "
              + "computed schema hash %s does not match entry %s",
              rowKey, getSchemaHash(entry.getSchema()), entry));
        }
        if (schemaId != entry.getId()) {
          LOG.error(String.format("Inconsistent schema ID table: "
              + "ID encoded in row key %d does not match entry: %s", schemaId, entry));
        }
      } catch (IOException ioe) {
        LOG.error(String.format(
            "Unable to decode schema ID table entry for row %s, timestamp %d: %s",
            rowKey, timestamp, ioe));
        continue;
      } catch (AvroRuntimeException are) {
        LOG.error(String.format(
            "Unable to decode schema ID table entry for row %s, timestamp %d: %s",
            rowKey, timestamp, are));
        continue;
      }
    }
    LOG.info(String.format(
        "Schema ID table has %d rows and %d entries.", idTableRowCounter, entries.size()));
    return entries;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraSchemaTable.class)
        .add("uri", mURI)
        .add("state", mState.get())
        .toString();
  }
}
