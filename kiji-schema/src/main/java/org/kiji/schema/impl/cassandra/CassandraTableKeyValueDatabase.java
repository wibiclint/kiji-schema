/**
 * (c) Copyright 2012 WibiData, Inc.
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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiTableKeyValueDatabase;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.KeyValueBackup;
import org.kiji.schema.avro.KeyValueBackupEntry;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.util.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
* Manages key-value pairs on a per table basis. Storage of these key-value pairs is provided by
* a column family of an HTable.
*/
@ApiAudience.Private
public class CassandraTableKeyValueDatabase
   implements KijiTableKeyValueDatabase<CassandraTableKeyValueDatabase> {

  public static final Logger LOG = LoggerFactory.getLogger(CassandraTableKeyValueDatabase.class);

  // TODO: final static vars for key, value column names in the C* table
  public static final String KV_COLUMN_TABLE = "table_name";
  public static final String KV_COLUMN_KEY = "key";
  public static final String KV_COLUMN_VALUE = "value";
  public static final String KV_COLUMN_TIME = "time";

  /**  The HBase table that stores Kiji metadata. */
  private final CassandraTableInterface mTable;

  /**
  * This class manages the storage and retrieval of key-value pairs on a per table basis. It is
  * backed by a C* table.
  *
  * @param cTable The table to store the key-value information in.
  */
  public CassandraTableKeyValueDatabase(CassandraTableInterface cTable) {
   mTable = Preconditions.checkNotNull(cTable);
  }

  /**
   * Install a table for user-defined key-value pairs.
   * @param admin A wrapper around an open C* session.
   * @param uri The KijiURI of the instance for this table.
   */
  public static void install(CassandraAdmin admin, KijiURI uri) {
    String tableName = KijiManagedCassandraTableName.getMetaKeyValueTableName(uri.getInstance()).toString();

    // Standard C* table layout.  Use text key + timestamp as composite primary key to allow selection by timestamp.
    String tableDescription = String.format(
        "(%s text, %s text, %s timeuuid, %s blob, PRIMARY KEY (%s, %s, %s));",
        KV_COLUMN_TABLE,
        KV_COLUMN_KEY,
        KV_COLUMN_TIME,
        KV_COLUMN_VALUE,
        KV_COLUMN_TABLE,
        KV_COLUMN_KEY,
        KV_COLUMN_TIME);
    admin.createTable(tableName, tableDescription);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String table, String key) throws IOException {
   final List<byte[]> values = getValues(table, key, 1);
   return values.get(0);
  }


  /**
   * Internal helper method containing common code for ready values for a given key from the table.
   * @param table Name of the table for which to fetch the values (part of the key-value database key).
   * @param key Name of the key for the KV store for the given table (part of the key-value database key).
   * @param numVersions Number of versions to fetch for the given table, key combination.
   * @return A list of C* rows for the query.
   */
  private List<Row> getRows(String table, String key, int numVersions) {
    Preconditions.checkArgument(numVersions >= 1,  "numVersions must be positive");

    String metaTableName = mTable.getTableName();
    Session session = mTable.getSession();

    // TODO: Prepare this statement once in constructor, not every load.
    String queryText = String.format(
        "SELECT * FROM %s WHERE %s=%s AND %s=%s ORDER BY %s DESC LIMIT 1",
        metaTableName,
        KV_COLUMN_TABLE,
        table,
        KV_COLUMN_KEY,
        key,
        KV_COLUMN_TIME
    );

    ResultSet resultSet = session.execute(queryText);
    List<Row> rows = resultSet.all();
    return rows;
  }

  /** {@inheritDoc} */
  @Override
  public List<byte[]> getValues(String table, String key, int numVersions) throws IOException {
    List<Row> rows = getRows(table, key, numVersions);
    if (0 == rows.size()) {
      throw new IOException(String.format(
          "Could not find any values associated with table %s and key %s", table, key));
    }

    // Convert result into a list of bytes
    final List<byte[]> values = Lists.newArrayList();
    for (Row row: rows) {
      ByteBuffer blob = row.getBytes(KV_COLUMN_VALUE);
      values.add(CassandraByteUtil.byteBuffertoBytes(blob));
    }
    return values;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, byte[]> getTimedValues(String table, String key, int numVersions)
      throws IOException {
    // TODO: Add the actual C* call to get data out of the table.

    List<Row> rows = getRows(table, key, numVersions);
    if (0 == rows.size()) {
      throw new IOException(String.format(
          "Could not find any values associated with table %s and key %s", table, key));
    }

    // Convert result into a map from timestamps to values
    final NavigableMap<Long, byte[]> timedValues = Maps.newTreeMap();
    for (Row row: rows) {
      ByteBuffer blob = row.getBytes(KV_COLUMN_VALUE);
      final byte[] bytes = CassandraByteUtil.byteBuffertoBytes(blob);
      Long timestamp = row.getLong(KV_COLUMN_TIME);
      Preconditions.checkState(timedValues.put(timestamp, bytes) == null);
    }
    return timedValues;
  }

  /** {@inheritDoc} */
  @Override
  public CassandraTableKeyValueDatabase putValue(String table, String key, byte[] value)
      throws IOException {
    String metaTableName = mTable.getTableName();
    Session session = mTable.getSession();

    // TODO: Make this query a member of the class and prepare in the constructor
    String queryText = String.format(
        "INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, now(), ?)",
        metaTableName,
        KV_COLUMN_TABLE,
        KV_COLUMN_KEY,
        KV_COLUMN_TIME,
        KV_COLUMN_VALUE);

    ByteBuffer valAsByteBuffer = CassandraByteUtil.bytesToByteBuffer(value);
    PreparedStatement preparedStatement = session.prepare(queryText);
    // TODO: Check for success?
    session.execute(preparedStatement.bind(table, key, valAsByteBuffer));
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void removeValues(String table, String key) throws IOException {
    String metaTableName = mTable.getTableName();
    Session session = mTable.getSession();

    // TODO: Make this query a member of the class and prepare in the constructor
    String queryText = String.format(
        "DELETE FROM %s WHERE %s=%s AND %s=%s",
        metaTableName,
        KV_COLUMN_TABLE,
        table,
        KV_COLUMN_KEY,
        key
    );

    // TODO: Check for success?
    session.execute(queryText);
  }

  /** {@inheritDoc} */
  @Override
  public Set<String> keySet(String table) throws IOException {
    // Just return a set of in-use keys
    // TODO: Make this query a member of the class and prepare in the constructor

    String metaTableName = mTable.getTableName();
    Session session = mTable.getSession();

    String queryText = String.format(
        "SELECT %s FROM %s WHERE %s=?",
        KV_COLUMN_KEY,
        metaTableName,
        KV_COLUMN_TABLE
    );

    PreparedStatement preparedStatement = session.prepare(queryText);
    ResultSet resultSet = session.execute(preparedStatement.bind(table));
    Set<String> keys = new HashSet<String>();

    for (Row row: resultSet.all()) {
      keys.add(row.getString(KV_COLUMN_KEY));
    }
    return keys;
}

  /** {@inheritDoc} */
  @Override
  public Set<String> tableSet() throws IOException {

    // Just return a set of in-use tables
    // TODO: Make this query a member of the class and prepare in the constructor

    String metaTableName = mTable.getTableName();
    Session session = mTable.getSession();

    String queryText = String.format(
        "SELECT %s FROM %s",
        KV_COLUMN_TABLE,
        metaTableName
    );

    ResultSet resultSet = session.execute(queryText);
    Set<String> keys = new HashSet<String>();

    for (Row row: resultSet.all()) {
      keys.add(row.getString(KV_COLUMN_TABLE));
    }
    return keys;
  }

 /** {@inheritDoc} */
 @Override
 public void removeAllValues(String table) throws IOException {
   Set<String> keysToRemove = keySet(table);
   for (String key : keysToRemove) {
     removeValues(table, key);
   }
 }

 /** {@inheritDoc} */
 @Override
 public KeyValueBackup keyValuesToBackup(String table) throws IOException {
   List<KeyValueBackupEntry> kvBackupEntries = Lists.newArrayList();
   final Set<String> keys = keySet(table);
   for (String key : keys) {
     NavigableMap<Long, byte[]> versionedValues = getTimedValues(table, key, Integer.MAX_VALUE);
     for (Long timestamp : versionedValues.descendingKeySet()) {
       kvBackupEntries.add(KeyValueBackupEntry.newBuilder()
           .setKey(key)
           .setValue(ByteBuffer.wrap(versionedValues.get(timestamp)))
           .setTimestamp(timestamp)
           .build());
     }
   }
   KeyValueBackup kvBackup = KeyValueBackup.newBuilder().setKeyValues(kvBackupEntries).build();
   return kvBackup;
 }

 /** {@inheritDoc} */
 @Override
 public void restoreKeyValuesFromBackup(final String tableName, KeyValueBackup keyValueBackup)
     throws IOException {
   LOG.debug(String.format("Restoring '%s' key-value(s) from backup for table '%s'.",
       keyValueBackup.getKeyValues().size(), tableName));

   String metaTableName = mTable.getTableName();
   Session session = mTable.getSession();
   // TODO: Make this query a member of the class and prepare in the constructor
   String queryText = String.format(
       "INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
       metaTableName,
       KV_COLUMN_TABLE,
       KV_COLUMN_KEY,
       KV_COLUMN_TIME,
       KV_COLUMN_VALUE);
   PreparedStatement preparedStatement = session.prepare(queryText);

   for (KeyValueBackupEntry kvRecord : keyValueBackup.getKeyValues()) {
     final String key = kvRecord.getKey();
     final ByteBuffer valAsByteBuffer = kvRecord.getValue(); // Read in ByteBuffer of values
     final long timestamp = kvRecord.getTimestamp();

     LOG.debug(String.format(
         "For the table '%s' we are writing key '%s', timestamp '%s', and value '%s' to the metatable named '%s'.",
         tableName,
         key,
         "" + timestamp,
         valAsByteBuffer.toString(),
         mTable.getTableName()));

     session.execute(preparedStatement.bind(tableName, key, new Date(timestamp), valAsByteBuffer));
   }
   LOG.debug("Flushing commits to restore key-values from backup.");
   // TODO: Any flush needed?
   //mTable.flushCommits();
 }
}
