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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import com.datastax.driver.core.Session;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SystemTableBackup;
import org.kiji.schema.avro.SystemTableEntry;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.impl.Versions;
import org.kiji.schema.util.CloseableIterable;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.util.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The Kiji system table that is stored in Cassandra.
 *
 * The system table (a Kiji system table) is a simple key-value store for system-wide
 * properties of a Kiji installation.  For the Cassandra implementation of Kiji, we store these keys
 * and values in a table called "kiji_system."
 *
 * For a key-value property (K,V), the key K is stored as the row key in the HTable, and the value V
 * is stored in the "value:" column.<p>
 */
@ApiAudience.Private
public class CassandraSystemTable implements KijiSystemTable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraSystemTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + CassandraSystemTable.class.getName());

  /** The Cassandra column family that stores the value of the properties. */
  public static final String VALUE_COLUMN_FAMILY = "value";

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

  /** The HTable that stores the Kiji instance properties. */
  //private final HTableInterface mTable;

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
   * Wrap an existing HTable connection that is assumed to be the table that stores the
   * Kiji instance properties.
   *
   * @param uri URI of the Kiji instance this table belongs to.
   */
  public CassandraSystemTable(KijiURI uri) {
    mURI = uri;

    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open SystemTable instance in state %s.", oldState);
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
    mTable.close();
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      CLEANUP_LOG.warn("Finalizing unclosed KijiSystemTable instance %s in state %s.", this, state);
      CLEANUP_LOG.debug("Stack when CassandraSystemTable was constructed:\n" + mConstructorStack);
      close();
    }
    super.finalize();
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getValue(String key) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get value from SystemTable instance in state %s.", state);
    Get get = new Get(Bytes.toBytes(key));
    get.addColumn(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
    Result result = mTable.get(get);
    return result.getValue(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0]);
  }

  /** {@inheritDoc} */
  @Override
  public void putValue(String key, byte[] value) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put value into SystemTable instance in state %s.", state);
    Put put = new Put(Bytes.toBytes(key));
    put.add(Bytes.toBytes(VALUE_COLUMN_FAMILY), new byte[0], value);
    mTable.put(put);
  }

  /** {@inheritDoc} */
  @Override
  public CloseableIterable<SimpleEntry<String, byte[]>> getAll() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get all from SystemTable instance in state %s.", state);
    return new CassandraSystemTableIterable(mTable.getScanner(Bytes.toBytes(VALUE_COLUMN_FAMILY)));
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
   * Installs a Kiji system table into a running Cassandra instance.
   *
   * @param session An open session connected to the Cassandra cluster to install to.
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @throws java.io.IOException If there is an error.
   */
  public static void install(
      // TODO: Instead of passing around an open session, might we want some kind of wrapper object
      // instead.
      Session session,
      KijiURI kijiURI,
      Configuration conf)
      throws IOException {
    install(session, kijiURI, conf, Collections.<String, String>emptyMap());
  }

  /**
   * Installs a Kiji system table into a running Cassandra instance.
   *
   * @param session An open session connected to the Cassandra cluster to install to.
   * @param kijiURI The KijiURI.
   * @param conf The Hadoop configuration.
   * @param properties The initial system properties to be used in addition to the defaults.
   * @throws java.io.IOException If there is an error.
   */
  public static void install(
      Session session,
      KijiURI kijiURI,
      Configuration conf,
      Map<String, String> properties)
      throws IOException {
    // Install the system table.
    String tableName = KijiManagedCassandraTableName
        .getSystemTableName(kijiURI.getInstance())
        .toString();

    String queryText = "CREATE TABLE " + tableName + " " +
        "(key text PRIMARY KEY, value blob);";

    // TODO: Check the results from this query?
    ResultSet resultSet = session.execute(queryText);

    CassandraSystemTable systemTable = new CassandraSystemTable(kijiURI);
    try {
      systemTable.loadSystemTableProperties(properties);
    } finally {
      ResourceUtils.closeOrLog(systemTable);
    }
  }

  /**
   * Disables and delete the system table from Cassandra.
   *
   * @param session An open session connected to the Cassandra cluster from which to uninstall.
   * @param kijiURI The URI for the kiji instance to remove.
   * @throws java.io.IOException If there is an error.
   */
  public static void uninstall(Session session, KijiURI kijiURI)
      throws IOException {
    final String tableName =
        KijiManagedCassandraTableName.getSystemTableName(kijiURI.getInstance()).toString();
    /*
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    */
    String tableName =
        KijiManagedCassandraTableName.getSystemTableName(kijiURI.getInstance()).toString();
    session.execute("DROP TABLE " + tableName);
  }

  /** {@inheritDoc} */
  @Override
  // TODO: Complete me!
  public SystemTableBackup toBackup() throws IOException {
    /*
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
    */
    return null;
  }

  /** {@inheritDoc} */
  @Override
  // TODO: Complete me!
  public void fromBackup(SystemTableBackup backup) throws IOException {
    /*
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot restore backup to SystemTable instance in state %s.", state);
    LOG.info(String.format("Restoring system table from backup with %d entries.",
        backup.getEntries().size()));
    for (SystemTableEntry entry : backup.getEntries()) {
      putValue(entry.getKey(), entry.getValue().array());
    }
    mTable.flushCommits();
    */
  }

  /** Private class for providing a CloseableIterable over system table key, value pairs. */
  // TODO: Complete me!

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraSystemTable.class)
        .add("uri", mURI)
        .add("state", mState.get())
        .toString();
  }
}
