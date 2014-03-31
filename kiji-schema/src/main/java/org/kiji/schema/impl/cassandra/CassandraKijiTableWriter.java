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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.KConstants;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;

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

  /** Contains shared code with BufferedWriter. */
  private final CassandraKijiWriterCommon mWriterCommon;

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
    private final CassandraColumnNameTranslator mTranslator;

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
        final CassandraColumnNameTranslator translator) {
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
          (CassandraColumnNameTranslator)capsule.getColumnNameTranslator());
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

    mAdmin = mTable.getAdmin();

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableWriter instance in state %s.", oldState);
    mWriterCommon = new CassandraKijiWriterCommon(mTable, mWriterLayoutCapsule);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    // Check whether this col is a counter; if so, do special counter write.
    if (isCounterColumn(family, qualifier)) {
      doCounterPut(entityId, family, qualifier, value);
    } else {
      put(entityId, family, qualifier, System.currentTimeMillis(), value);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to KijiTableWriter instance %s in state %s.", this, state);

    // Check whether this col is a counter; if so, do special counter write.
    if (isCounterColumn(family, qualifier)) {
      throw new UnsupportedOperationException("Cannot specify a timestamp during a counter put.");
    }

    Statement putStatement = mWriterCommon.getStatementPutNotCounter(
        entityId, family, qualifier, timestamp, value);
    mAdmin.execute(putStatement);
  }

  // ----------------------------------------------------------------------------------------------
  // Counter set, get, increment.

  private boolean isCounterColumn(String family, String qualifier) throws IOException {
    return mWriterLayoutCapsule
        .getLayout()
        .getCellSpec(new KijiColumnName(family, qualifier))
        .isCounter();
  }

  private long getCounterValue(
      EntityId entityId,
      String family,
      String qualifier
  ) throws IOException {
    // Encode the EntityId as a ByteBuffer for C*.
    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    // Get a reference to the full name of the C* table for this column.
    KijiManagedCassandraTableName cTableName =
        KijiManagedCassandraTableName.getKijiCounterTableName(mTable.getURI(), mTable.getName());

    // TODO: Refactor this query text (and preparation for it) elsewhere.
    // Create the CQL statement to read back the counter value.
    String queryText = String.format(
        "SELECT * FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=?",
        cTableName.toString(),
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL);

    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) mWriterLayoutCapsule.getColumnNameTranslator();

    PreparedStatement preparedStatement = mAdmin.getPreparedStatement(queryText);
    ResultSet resultSet = mAdmin.execute(preparedStatement.bind(
        rowKey,
        translator.toCassandraLocalityGroup(columnName),
        translator.toCassandraColumnFamily(columnName),
        translator.toCassandraColumnQualifier(columnName)
    ));

    List<Row> readCounterResults = resultSet.all();

    long currentCounterValue;
    if (readCounterResults.isEmpty()) {
      currentCounterValue = 0; // Uninitialized counter, effectively a counter at 0.
    } else if (1 == readCounterResults.size()) {
      currentCounterValue = readCounterResults.get(0).getLong(CassandraKiji.CASSANDRA_VALUE_COL);
    } else {
      // TODO: Handle this appropriately, this should never happen!
      throw new KijiIOException("Should not have multiple values for a counter!");
    }
    return currentCounterValue;
  }

  private <T> void incrementCounterValue(
      EntityId entityId,
      String family,
      String qualifier,
      long counterIncrement) throws IOException {
    LOG.info("Incrementing the counter by " + counterIncrement);

    // Encode the EntityId as a ByteBuffer for C*.
    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    // Get a reference to the full name of the C* table for this column.
    KijiManagedCassandraTableName cTableName =
        KijiManagedCassandraTableName.getKijiCounterTableName(mTable.getURI(), mTable.getName());

    // TODO: Refactor this query text (and preparation for it) elsewhere.
    // Create the CQL statement to insert data.
    String queryText = String.format(
        "UPDATE %s SET %s = %s + ? WHERE %s=? AND %s=? AND %s=? AND %s=? AND %s=?",
        cTableName.toString(),
        CassandraKiji.CASSANDRA_VALUE_COL,
        CassandraKiji.CASSANDRA_VALUE_COL,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL);

    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) mWriterLayoutCapsule.getColumnNameTranslator();

    PreparedStatement preparedStatement = mAdmin.getPreparedStatement(queryText);
    mAdmin.execute(preparedStatement.bind(
        counterIncrement,
        rowKey,
        translator.toCassandraLocalityGroup(columnName),
        translator.toCassandraColumnFamily(columnName),
        translator.toCassandraColumnQualifier(columnName),
        KConstants.CASSANDRA_COUNTER_TIMESTAMP
    ));
  }

  private <T> void doCounterPut(
      EntityId entityId,
      String family,
      String qualifier,
      T value) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to KijiTableWriter instance %s in state %s.", this, state);

    LOG.info("-------------------- Performing a put to a counter. --------------------");
    LOG.info(String.format("(%s, %s, %s) := %s", entityId, family, qualifier, value));

    // TODO: Assert that "value" is a long.

    // Read back the current value of the counter.
    long currentCounterValue = getCounterValue(entityId, family, qualifier);
    LOG.info("Current value of counter is " + currentCounterValue);
    // Increment the counter appropriately to get the new value.
    long counterIncrement = (Long) value - currentCounterValue;
    incrementCounterValue(entityId, family, qualifier, counterIncrement);

  }

  /** {@inheritDoc} */
  @Override
  public KijiCell<Long> increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot increment cell to KijiTableWriter instance %s in state %s.", this, state);

    if (!isCounterColumn(family, qualifier)) {
      throw new UnsupportedOperationException(String.format("Column '%s:%s' is not a counter", family, qualifier));
    }

    // Increment the counter appropriately to get the new value.
    incrementCounterValue(entityId, family, qualifier, amount);

    // Read back the current value of the counter.
    long currentCounterValue = getCounterValue(entityId, family, qualifier);
    LOG.info("Value of counter after increment is " + currentCounterValue);

    final DecodedCell<Long> counter =
        new DecodedCell<Long>(null, currentCounterValue);
    return new KijiCell<Long>(family, qualifier, KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter);
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete row while KijiTableWriter %s is in state %s.", this, state);
    // TODO: Could check whether this family has an non-counter / counter columns before delete.
    // TODO: Should we wait for these calls to complete before returning?
    mAdmin.executeAsync(mWriterCommon.getStatementDeleteRow(entityId));
    mAdmin.executeAsync(mWriterCommon.getStatementDeleteRowCounter(entityId));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    throw new UnsupportedOperationException("Cannot delete with up-to timestamp in Cassandra Kiji.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete family while KijiTableWriter %s is in state %s.", this, state);

    // TODO: Could check whether this family has an non-counter / counter columns before delete.
    // TODO: Should we wait for these calls to complete before returning?
    mAdmin.executeAsync(mWriterCommon.getStatementDeleteFamily(entityId, family));
    mAdmin.executeAsync(mWriterCommon.getStatementDeleteFamilyCounter(entityId, family));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {
    throw new UnsupportedOperationException("Cannot delete with up-to timestamp in Cassandra Kiji.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete column while KijiTableWriter %s is in state %s.", this, state);
    Statement statement = mWriterCommon.getStatementDeleteColumn(entityId, family, qualifier);
    mAdmin.execute(statement);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    throw new UnsupportedOperationException("Cannot delete with up-to timestamp in Cassandra Kiji.");
  }

  /**
   * Handles deleting individual cells in counter or non-counter Cassandra tables.
   *
   * @param entityId
   * @param family
   * @param qualifier
   * @throws IOException
   */
  private void doDeleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    Statement statement =
        mWriterCommon.getStatementDeleteCell(entityId, family, qualifier, timestamp);
    mAdmin.execute(statement);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier) throws IOException {
    if (isCounterColumn(family, qualifier)) {
      doDeleteCell(entityId, family, qualifier, KConstants.CASSANDRA_COUNTER_TIMESTAMP);
    } else {
      throw new UnsupportedOperationException(
        "Cannot delete only most-recent version of a cell in Cassandra Kiji."
      );
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete cell while KijiTableWriter %s is in state %s.", this, state);

    if (isCounterColumn(family, qualifier)) {
      // TODO: Better error message.
      throw new UnsupportedOperationException("Cannot delete counter with specified timestamp.");
    }

    doDeleteCell(entityId, family, qualifier, timestamp);
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
