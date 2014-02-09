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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.hbase.HBaseKijiTable;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Makes modifications to a Kiji table by sending requests directly to HBase from the local client.
 *
 * <p> This writer flushes immediately to HBase, so there is no need to call flush() explicitly.
 * All put, increment, delete, and verify operations will cause a synchronous RPC call to HBase.
 * </p>
 * <p> This writer acquires a dedicated HTable object for its entire life span. </p>
 * <p> This class is not thread-safe and must be synchronized externally. </p>
 */
@ApiAudience.Private
public final class CassandraKijiTableWriter implements KijiTableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTableWriter.class);

  /** The kiji table instance. */
  private final CassandraKijiTable mTable;

  /** C* Admin instance, used for executing CQL commands. */
  private final CassandraAdmin mAdmin;

  /** States of a writer instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this writer. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Processes layout update from the KijiTable to which this writer writes. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /** Dedicated HTable connection. */
  //private final CassandraTableInterface mCTable;

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /**
   * A container for all writer state which should be modified atomically to reflect an update to
   * the underlying table's layout.
   */
  public static final class WriterLayoutCapsule {
    private final CellEncoderProvider mCellEncoderProvider;
    private final KijiTableLayout mLayout;
    private final ColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellEncoderProvider the encoder provider to store in this container.
     * @param layout the table layout to store in this container.
     * @param translator the column name translator to store in this container.
     */
    public WriterLayoutCapsule(
        final CellEncoderProvider cellEncoderProvider,
        final KijiTableLayout layout,
        final ColumnNameTranslator translator) {
      mCellEncoderProvider = cellEncoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the column name translator from this container.
     *
     * @return the column name translator from this container.
     */
    public ColumnNameTranslator getColumnNameTranslator() {
      return mTranslator;
    }

    /**
     * Get the table layout from this container.
     *
     * @return the table layout from this container.
     */
    public KijiTableLayout getLayout() {
      return mLayout;
    }

    /**
     * get the encoder provider from this container.
     *
     * @return the encoder provider from this container.
     */
    public CellEncoderProvider getCellEncoderProvider() {
      return mCellEncoderProvider;
    }
  }

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final LayoutCapsule capsule) throws IOException {
      final State state = mState.get();
      if (state == State.CLOSED) {
        LOG.debug("Writer is closed: ignoring layout update.");
        return;
      }
      final CellEncoderProvider provider = new CellEncoderProvider(
          mTable.getURI(),
          capsule.getLayout(),
          mTable.getKiji().getSchemaTable(),
          DefaultKijiCellEncoderFactory.get());
      // If the capsule is null this is the initial setup and we do not need a log message.
      if (mWriterLayoutCapsule != null) {
        LOG.debug(
            "Updating layout used by KijiTableWriter: {} for table: {} from version: {} to: {}",
            this,
            mTable.getURI(),
            mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
            capsule.getLayout().getDesc().getLayoutId());
      } else {
        LOG.debug("Initializing KijiTableWriter: {} for table: {} with table layout version: {}",
            this,
            mTable.getURI(),
            capsule.getLayout().getDesc().getLayoutId());
      }
      // Normally we would atomically flush and update mWriterLayoutCapsule here,
      // but since this writer is unbuffered, the flush is unnecessary
      mWriterLayoutCapsule = new WriterLayoutCapsule(
          provider,
          capsule.getLayout(),
          capsule.getColumnNameTranslator());
    }
  }

  /**
   * Creates a non-buffered kiji table writer that sends modifications directly to Kiji.
   *
   * @param table A kiji table.
   * @throws java.io.IOException on I/O error.
   */
  public CassandraKijiTableWriter(CassandraKijiTable table) throws IOException {
    mTable = table;
    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "KijiTableWriter for table: %s failed to initialize.", mTable.getURI());

    // TODO: Do we need to do something similar here?
    //mCTable = table.openCassandraTableConnection();
    //SchemaPlatformBridge.get().setAutoFlush(mCTable, true);
    mAdmin = mTable.getAdmin();

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableWriter instance in state %s.", oldState);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    put(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to KijiTableWriter instance %s in state %s.", this, state);

    // TODO: Refactor Kiji/C* mapping code to common place.

    // TODO: Implement column name translations (not present now).

    // Encode the value to write into the table as a ByteBuffer for C*.
    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);
    final ByteBuffer encodedByteBuffer = CassandraByteUtil.bytesToByteBuffer(encoded);

    // Encode the EntityId as a ByteBuffer for C*.
    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    // Get a reference to the full name of the C* table for this column.
    // TODO: Refactor this name-creation code somewhere cleaner.
    KijiManagedCassandraTableName cTableName = KijiManagedCassandraTableName.getKijiTableName(
        mTable.getURI(),
        mTable.getName()
    );


    // TODO: Refactor this query text (and preparation for it) elsewhere.
    // Create the CQL statement to insert data.
    String queryText = String.format(
        "INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?);",
        cTableName.toString(),
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL,
        CassandraKiji.CASSANDRA_VALUE_COL);
    LOG.info(queryText);

    Session session = mAdmin.getSession();

    PreparedStatement preparedStatement = session.prepare(queryText);
    session.execute(preparedStatement.bind(
        rowKey,
        family,
        qualifier,
        timestamp,
        encodedByteBuffer
    ));
  }

  // ----------------------------------------------------------------------------------------------
  // Counter increment

  /** {@inheritDoc} */
  @Override
  public KijiCell<Long> increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot increment cell to KijiTableWriter instance %s in state %s.", this, state);

    verifyIsCounter(family, qualifier);

    // TODO: Write actual implementation for incrementing C* Kiji counter.

    /*
    // Translate the Kiji column name to an HBase column name.
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator().
        toHBaseColumnName(new KijiColumnName(family, qualifier));

    // Send the increment to the HBase HTable.
    final Increment increment = new Increment(entityId.getHBaseRowKey());
    increment.addColumn(
        hbaseColumnName.getFamily(),
        hbaseColumnName.getQualifier(),
        amount);
    final Result result = mHTable.increment(increment);
    final NavigableMap<Long, byte[]> counterEntries =
        result.getMap().get(hbaseColumnName.getFamily()).get(hbaseColumnName.getQualifier());
    assert null != counterEntries;
    assert 1 == counterEntries.size();

    final Map.Entry<Long, byte[]> counterEntry = counterEntries.firstEntry();
    final DecodedCell<Long> counter =
        new DecodedCell<Long>(null, Bytes.toLong(counterEntry.getValue()));
    return new KijiCell<Long>(family, qualifier, counterEntry.getKey(), counter);
    */
    return null;
  }

  /**
   * Verifies that a column is a counter.
   *
   * @param family A column family.
   * @param qualifier A column qualifier.
   * @throws java.io.IOException If the column is not a counter, or it does not exist.
   */
  private void verifyIsCounter(String family, String qualifier) throws IOException {
    // TODO: Write actual implementation for verifying that a column is a C* counter.
    /*
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    if (mWriterLayoutCapsule.getLayout().getCellSchema(column).getType() != SchemaType.COUNTER) {
      throw new IOException(String.format("Column '%s' is not a counter", column));
    }
    */
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    deleteRow(entityId, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete row while KijiTableWriter %s is in state %s.", this, state);

    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());
    KijiManagedCassandraTableName cTableName = KijiManagedCassandraTableName.getKijiTableName(
        mTable.getURI(),
        mTable.getName()
    );

    // TODO: Prepare this statement first.
    String queryString = String.format(
        "DELETE FROM %s WHERE %s=?",
        cTableName.toString(),
        CassandraKiji.CASSANDRA_KEY_COL
    );

    Session session = mAdmin.getSession();
    PreparedStatement preparedStatement = session.prepare(queryString);
    session.execute(preparedStatement.bind(rowKey));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family) throws IOException {
    deleteFamily(entityId, family, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete family while KijiTableWriter %s is in state %s.", this, state);

    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final FamilyLayout familyLayout = capsule.getLayout().getFamilyMap().get(family);
    if (null == familyLayout) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }

    // TODO: Column name translation.
    //final HBaseColumnName hbaseColumnName = capsule.getColumnNameTranslator()
        //.toHBaseColumnName(new KijiColumnName(family));

    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());
    KijiManagedCassandraTableName cTableName = KijiManagedCassandraTableName.getKijiTableName(
        mTable.getURI(),
        mTable.getName()
    );

    // TODO: Prepare this statement first.
    String queryString = String.format(
        "DELETE FROM %s WHERE %s=? AND %s=? AND %s <= ?",
        cTableName.toString(),
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_VERSION_COL
    );

    Session session = mAdmin.getSession();
    PreparedStatement preparedStatement = session.prepare(queryString);
    session.execute(preparedStatement.bind(rowKey, family, upToTimestamp));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier) throws IOException {
    deleteColumn(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete column while KijiTableWriter %s is in state %s.", this, state);

    // TODO: Column name translation.
    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());
    KijiManagedCassandraTableName cTableName = KijiManagedCassandraTableName.getKijiTableName(
        mTable.getURI(),
        mTable.getName()
    );

    // TODO: Prepare this statement first.
    String queryString = String.format(
        "DELETE FROM %s WHERE %s=? AND %s=? AND %s=? AND %s <= ?",
        cTableName.toString(),
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL
    );

    Session session = mAdmin.getSession();
    PreparedStatement preparedStatement = session.prepare(queryString);
    session.execute(preparedStatement.bind(rowKey, family, qualifier, upToTimestamp));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier) throws IOException {
    deleteCell(entityId, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete cell while KijiTableWriter %s is in state %s.", this, state);

    // TODO: Column name translation.
    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());
    KijiManagedCassandraTableName cTableName = KijiManagedCassandraTableName.getKijiTableName(
        mTable.getURI(),
        mTable.getName()
    );

    // TODO: Prepare this statement first.
    String queryString = String.format(
        "DELETE FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=?",
        cTableName.toString(),
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL
    );

    Session session = mAdmin.getSession();
    PreparedStatement preparedStatement = session.prepare(queryString);
    session.execute(preparedStatement.bind(rowKey, family, qualifier, timestamp));
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    LOG.debug("KijiTableWriter does not need to be flushed.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiTableWriter instance %s in state %s.", this, oldState);
    mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
    // TODO: May need a close call here for reference counting of Cassandra tables.
    //mHTable.close();
    mTable.release();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraKijiTableWriter.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .add("layout-version", mWriterLayoutCapsule.getLayout().getDesc().getLayoutId())
        .add("state", mState)
        .toString();
  }
}
