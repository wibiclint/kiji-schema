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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SystemTableBackup;
import org.kiji.schema.avro.SystemTableEntry;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.util.CloseableIterable;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;

/**
 * <p>The Kiji system table that is stored in Cassandra.</p>
 *
 * <p>The system table (a Kiji system table) is a simple key-value store for system-wide
 * properties of a Kiji installation.  There is a single column family "value".  For a
 * key-value property (K,V), the key K is stored as the row key in the Cassandra table,
 * and the value V is stored in the "value:" column.<p>
 */
@ApiAudience.Private
public final class CassandraSystemTable implements KijiSystemTable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSystemTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + CassandraSystemTable.class.getName());

  /** The Cassandra column family that stores the value of the properties. */
  public static final String KEY_COLUMN = "key";
  public static final String VALUE_COLUMN = "value";

  /** The Cassandra row key that stores the installed Kiji data format version. */
  public static final String KEY_DATA_VERSION = "data-version";

  /** The Cassandra row key that stores the Kiji security version. */
  public static final String SECURITY_PROTOCOL_VERSION = "security-version";

  /**
   * The name of the file that stores the current system table defaults that are loaded
   * at installation time.
   */
  public static final String DEFAULTS_PROPERTIES_FILE =
      "org/kiji/schema/system-default.properties";

  /** URI of the Kiji instance this system table belongs to. */
  private final KijiURI mURI;

  /** The Cassandra table that stores the Kiji instance properties. */
  private final CassandraTableInterface mTable;

  /** States of a SystemTable instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this SystemTable instance. */
  private AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Used for testing finalize() behavior. */
  private String mConstructorStack = "";

  /**
   * Creates a new CassandraTableInterface for the Kiji system table.
   *
   * This method assumes that the system table already exists.
   *
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @param admin Wrapper around open C* session.
   * @return a new CassandraTableInterface for the Kiji system table.
   * @throws IOException on I/O error.
   */
  public static CassandraTableInterface newSystemTable(
      KijiURI kijiURI,
      Configuration conf,
      CassandraAdmin admin)
      throws IOException {
    final String tableName =
        KijiManagedCassandraTableName.getSystemTableName(kijiURI).toString();
    // Check that the table already exists!
    if (!admin.tableExists(tableName)) {
      LOG.info("Cannot find table " + tableName + ", assuming Kiji not installed...");
      throw new KijiNotInstalledException(
          String.format("Kiji instance %s is not installed.", kijiURI),
          kijiURI);
    }
    return admin.getCassandraTableInterface(tableName);
  }

  /**
   * Wrap an existing Cassandra table that is assumed to be the table that stores the
   * Kiji instance properties.
   *
   * This method assumes that the system table already exists.
   *
   * @param kijiURI URI of the Kiji instance this table belongs to.
   * @param conf Hadoop configuration (not used now)
   * @param admin Wrapper around open C* session.
   * @return a reference to the system table.
   * @throws java.io.IOException if there is a problem communicating with Cassandra.
   */
  public static CassandraSystemTable createAssumingTableExists(
      KijiURI kijiURI,
      Configuration conf,
      CassandraAdmin admin)
    throws IOException {
    return new CassandraSystemTable(kijiURI, newSystemTable(kijiURI, conf, admin));
  }

  private final PreparedStatement mPreparedStatementGetValue;
  private final PreparedStatement mPreparedStatementPutValue;

  /**
   * Wrap an existing Cassandra table that is assumed to be the table that stores the
   * Kiji instance properties.
   *
   * @param uri URI of the Kiji instance this table belongs to.
   * @param ctable A C* table to wrap.
   */
  private CassandraSystemTable(KijiURI uri, CassandraTableInterface ctable) {
    mURI = uri;
    mTable = ctable;

    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open SystemTable instance in state %s.", oldState);

    String queryText;

    // Prepare some statements for CQL queries
    queryText = "SELECT " + VALUE_COLUMN + " FROM " +  mTable.getTableName()
        + " WHERE " + KEY_COLUMN + "=?";
    mPreparedStatementGetValue = mTable.getAdmin().getPreparedStatement(queryText);

    queryText = "INSERT INTO " +  mTable.getTableName() + "(" + KEY_COLUMN + "," + VALUE_COLUMN
        + ") VALUES(?,?);";
    mPreparedStatementPutValue = mTable.getAdmin().getPreparedStatement(queryText);

  }

  /** {@inheritDoc} */
  @Override
  public synchronized ProtocolVersion getDataVersion() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get data version from SystemTable instance in state %s.", state);
    byte[] result = getValue(KEY_DATA_VERSION);
    return result == null ? null : ProtocolVersion.parse(Bytes.toString(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setDataVersion(ProtocolVersion version) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot set data version in SystemTable instance in state %s.", state);
    putValue(KEY_DATA_VERSION, Bytes.toBytes(version.toString()));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ProtocolVersion getSecurityVersion() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get security version from SystemTable instance in state %s.", state);
    byte[] result = getValue(SECURITY_PROTOCOL_VERSION);
    return result == null
        ? Versions.UNINSTALLED_SECURITY_VERSION
        : ProtocolVersion.parse(Bytes.toString(result));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void setSecurityVersion(ProtocolVersion version) throws IOException {
    Preconditions.checkNotNull(version);
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot set security version in SystemTable instance in state %s.", state);
    Kiji.Factory.open(mURI).getSecurityManager().checkCurrentGrantAccess();
    putValue(SECURITY_PROTOCOL_VERSION, Bytes.toBytes(version.toString()));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiSystemTable instance in state %s.", oldState);
    // TODO: Do something here?
    //mTable.close();
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      CLEANUP_LOG.warn("Finalizing unclosed KijiSystemTable instance %s in state %s.", this, state);
      CLEANUP_LOG.debug("Stack when HBaseSystemTable was constructed:\n" + mConstructorStack);
      close();
    }
    super.finalize();
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String key) throws IOException {
    LOG.info(String.format("Reading value for key = %s", key));
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get value from SystemTable instance in state %s.", state);
    ResultSet resultSet = mTable.getAdmin().execute(mPreparedStatementGetValue.bind(key));

    // Extra the value from the byte buffer, otherwise return this empty buffer
    // TODO: Some additional sanity checks here?
    List<Row> rows = resultSet.all();
    Preconditions.checkArgument(
        rows.size() <= 1,
        "Expected to get back exactly zero or one rows from query to get value from system table, "
          + "but got back " + rows
    );
    if (rows.size() == 1) {
      Row row = rows.get(0);
      return CassandraByteUtil.byteBuffertoBytes(row.getBytes(VALUE_COLUMN));
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void putValue(String key, byte[] value) throws IOException {
    //LOG.info(String.format("Putting key, value = %s,%s", key, value));
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put value into SystemTable instance in state %s.", state);
    ByteBuffer valAsByteBuffer = CassandraByteUtil.bytesToByteBuffer(value);
    // TODO: Check for success?
    mTable.getAdmin().execute(mPreparedStatementPutValue.bind(key, valAsByteBuffer));
  }

  /** {@inheritDoc} */
  @Override
  public CloseableIterable<SimpleEntry<String, byte[]>> getAll() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get all from SystemTable instance in state %s.", state);

    // TODO: Make this a prepared query.
    String queryText = "SELECT * FROM " +  mTable.getTableName() + ";";
    ResultSet resultSet = mTable.getAdmin().execute(queryText);

    // Extra the value from the byte buffer, otherwise return this empty buffer
    // TODO: Some checks here?
    return new CassandraSystemTableIterable(resultSet);
  }

  /**
   * Loads a map of properties from the properties file named by resource.
   *
   * @param resource The name of the properties resource holding the defaults.
   * @return The properties in the file as a Map.
   * @throws java.io.IOException If there is an error.
   */
  public static Map<String, String> loadPropertiesFromFileToMap(String resource)
      throws IOException {
    final Properties defaults = new Properties();
    defaults.load(CassandraSystemTable.class.getClassLoader().getResourceAsStream(resource));
    return Maps.fromProperties(defaults);
  }

  /**
   * Load the system table with the key/value pairs specified in properties.  Default properties are
   * loaded for any not specified.
   *
   * @param properties The properties to load into the system table.
   * @throws java.io.IOException If there is an I/O error.
   */
  protected void loadSystemTableProperties(Map<String, String> properties) throws IOException {
    final Map<String, String> defaults = loadPropertiesFromFileToMap(DEFAULTS_PROPERTIES_FILE);
    final Map<String, String> newProperties = Maps.newHashMap(defaults);
    newProperties.putAll(properties);
    for (Map.Entry<String, String> entry : newProperties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      putValue(key, Bytes.toBytes(value));
    }
  }

  /**
   * Installs a Kiji system table into a running HBase instance.
   *
   * @param admin The HBase cluster to install into.
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @throws java.io.IOException If there is an error.
   */
  public static void install(
      CassandraAdmin admin,
      KijiURI kijiURI,
      Configuration conf)
      throws IOException {
    install(admin, kijiURI, conf, Collections.<String, String>emptyMap());
  }

  /**
   * Installs a Kiji system table into a running C* instance.
   *
   * @param admin The Cassandra cluster and keyspace install into.
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration (not clear if we need this or not).
   * @param properties The initial system properties to be used in addition to the defaults.
   * @throws java.io.IOException If there is an error.
   */
  public static void install(
      CassandraAdmin admin,
      KijiURI kijiURI,
      Configuration conf,
      Map<String, String> properties)
      throws IOException {
    // Install the table.  Sadly, we have to just use blobs and byte arrays here, so that we are
    // compliant with everything else in Kiji.  :(
    String systemTableName =
        KijiManagedCassandraTableName.getSystemTableName(kijiURI).toString();

    // The layout of this table is straightforward - just blob to blob!
    // TODO: Any check here first for whether the table exists?
    String tableLayout =
        String.format("CREATE TABLE %s (%s text PRIMARY KEY, %s blob);",
            systemTableName, KEY_COLUMN, VALUE_COLUMN);

    admin.createTable(systemTableName, tableLayout);
    CassandraTableInterface ctable = admin.getCassandraTableInterface(systemTableName);

    CassandraSystemTable systemTable = new CassandraSystemTable(kijiURI, ctable);
    try {
      systemTable.loadSystemTableProperties(properties);
    } finally {
      ResourceUtils.closeOrLog(systemTable);
    }
  }

  /**
   * Disables and delete the system table from HBase.
   *
   * @param admin The HBase admin object.
   * @param kijiURI The URI for the kiji instance to remove.
   * @throws java.io.IOException If there is an error.
   */
  public static void uninstall(CassandraAdmin admin, KijiURI kijiURI)
      throws IOException {
    // TODO: Does this actually need to do anything beyond dropping the table?
    final String tableName =
        KijiManagedCassandraTableName.getSystemTableName(kijiURI).toString();
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public SystemTableBackup toBackup() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot backup SystemTable instance in state %s.", state);
    ArrayList<SystemTableEntry> backupEntries = new ArrayList<SystemTableEntry>();
    CloseableIterable<SimpleEntry<String, byte[]>> entries = getAll();
    for (SimpleEntry<String, byte[]> entry : entries) {
      backupEntries.add(SystemTableEntry.newBuilder()
          .setKey(entry.getKey())
          .setValue(ByteBuffer.wrap(entry.getValue()))
          .build());
    }

    return SystemTableBackup.newBuilder().setEntries(backupEntries).build();
  }

  /** {@inheritDoc} */
  @Override
  public void fromBackup(SystemTableBackup backup) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot restore backup to SystemTable instance in state %s.", state);
    LOG.info(String.format("Restoring system table from backup with %d entries.",
        backup.getEntries().size()));
    for (SystemTableEntry entry : backup.getEntries()) {
      putValue(entry.getKey(), entry.getValue().array());
    }
    // TODO: Flush?
    //mTable.flushCommits();
  }

  /** Private class for providing a CloseableIterable over system table key, value pairs. */
  private static class CassandraSystemTableIterable
      implements CloseableIterable<SimpleEntry<String, byte[]>> {

    /** Uderlying source of system table parameters. */
    //private ResultScanner mResultScanner;

    /** Iterator returned by iterator(). */
    private Iterator<SimpleEntry<String, byte[]>> mIterator;

    /**
     * Create a new CassandraSystemTableIterable across system table properties.
     *
     * @param resultSet scanner across the target cells.
     */
    public CassandraSystemTableIterable(ResultSet resultSet) {
      mIterator = new CassandraSystemTableIterator(resultSet.iterator());
      //mResultScanner = resultScanner;
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<SimpleEntry<String, byte[]>> iterator() {
      return mIterator;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      //mResultScanner.close();
    }
  }

  /** Private class for providing an Iterator to HBaseSystemTableIterable. */
  private static class CassandraSystemTableIterator
      implements Iterator<SimpleEntry<String, byte[]>> {

    /**
     * Iterator across result scanner results.
     * Used to build next() for HBaseSystemTableIterator
     */
    private Iterator<Row> mRowIterator;

    /**
     * Create an HBaseSystemTableIterator across the results of a ResultScanner.
     *
     * @param rowIterator iterator across the scanned cells.
     */
    public CassandraSystemTableIterator(Iterator<Row> rowIterator) {
      mRowIterator = rowIterator;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return mRowIterator.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public SimpleEntry<String, byte[]> next() {
      Row next = mRowIterator.next();
      String key = next.getString(KEY_COLUMN);
      byte[] value = CassandraByteUtil.byteBuffertoBytes(next.getBytes(VALUE_COLUMN));
      return new SimpleEntry<String, byte[]>(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraSystemTable.class)
        .add("uri", mURI)
        .add("state", mState.get())
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getKijiURI() {
    return mURI;
  }
}
