/**
 * (c) Copyright 2013 WibiData, Inc.
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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.hbase.HBaseKijiTableWriter.WriterLayoutCapsule;
import org.kiji.schema.layout.LayoutUpdatedException;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cassandra implementation of AtomicKijiPutter.
 *
 * Facilitates guaranteed atomic puts in batch on a single row.
 *
 * Use <code>begin(EntityId)</code> to open a new transaction,
 * <code>put(family, qualifier, value)</code> to stage a put in the transaction,
 * and <code>commit()</code> or <code>checkAndCommit(family, qualifier, value)</code>
 * to write all staged puts atomically.
 *
 * This class is not thread-safe.  It is the user's responsibility to protect against
 * concurrent access to a writer while a transaction is being constructed.
 */
@ApiAudience.Private
public final class CassandraAtomicKijiPutter implements AtomicKijiPutter {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraAtomicKijiPutter.class);

  /** The Kiji table instance. */
  private final CassandraKijiTable mTable;

  /** States of an atomic kiji putter instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this atomic kiji putter. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Object which processes layout update from the KijiTable to which this Writer writes. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /** Lock for synchronizing layout update mutations. */
  private final Object mLock = new Object();

  /** EntityId of the row to mutate atomically. */
  private EntityId mEntityId;

  /** List of cells to be written. */
  private ArrayList<CassandraKeyValue> mHopper = null;

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /**
   * <p>
   *   Set to true when the table calls
   *   {@link
   *   InnerLayoutUpdater#update(org.kiji.schema.impl.LayoutCapsule)}
   *   to indicate a table layout update.  Set to false when a user calls
   *   {@link #begin(org.kiji.schema.EntityId)}.  If this becomes true while a transaction is in
   *   progress all methods which would advance the transaction will instead call
   *   {@link #rollback()} and throw a {@link org.kiji.schema.layout.LayoutUpdatedException}.
   * </p>
   * <p>
   *   Access to this variable must be protected by synchronizing on mLock.
   * </p>
   */
  private boolean mLayoutOutOfDate = false;

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final LayoutCapsule capsule) throws IOException {
      final State state = mState.get();
      Preconditions.checkState(state != State.CLOSED,
          "Cannot update an AtomicKijiPutter instance in state %s.", state);
      synchronized (mLock) {
        mLayoutOutOfDate = true;
        // Update the state of the writer.
        final CellEncoderProvider provider = new CellEncoderProvider(
            mTable.getURI(),
            capsule.getLayout(),
            mTable.getKiji().getSchemaTable(),
            DefaultKijiCellEncoderFactory.get());
        // If the capsule is null this is the initial setup and we do not need a log message.
        if (mWriterLayoutCapsule != null) {
          LOG.debug(
              "Updating layout used by AtomicKijiPutter: {} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
              capsule.getLayout().getDesc().getLayoutId());
        } else {
          LOG.debug("Initializing AtomicKijiPutter: {} for table: {} with table layout version: {}",
              this,
              mTable.getURI(),
              capsule.getLayout().getDesc().getLayoutId());
        }
        mWriterLayoutCapsule = new WriterLayoutCapsule(
            provider,
            capsule.getLayout(),
            capsule.getColumnNameTranslator());
      }
    }
  }

  /**
   * Constructor for this AtomicKijiPutter.
   *
   * @param table The CassandraKijiTable to which this writer writes.
   * @throws java.io.IOException in case of an error.
   */
  public CassandraAtomicKijiPutter(CassandraKijiTable table) throws IOException {
    mTable = table;
    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "AtomicKijiPutter for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    table.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open AtomicKijiPutter instance in state %s.", oldState);
  }

  /** Resets the current transaction. */
  private void reset() {
    mEntityId = null;
    mHopper = null;
  }

  /** {@inheritDoc} */
  @Override
  public void begin(EntityId eid) {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot begin a transaction on an AtomicKijiPutter instance in state %s.", state);
    // Preconditions.checkArgument() cannot be used here because mEntityId is null between calls to
    // begin().
    if (mHopper != null) {
      throw new IllegalStateException(String.format("There is already a transaction in progress on "
          + "row: %s. Call commit(), checkAndCommit(), or rollback() to clear the Put.",
          mEntityId.toShellString()));
    }
    synchronized (mLock) {
      mLayoutOutOfDate = false;
    }
    mEntityId = eid;
    mHopper = new ArrayList<CassandraKeyValue>();
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /** {@inheritDoc} */
  @Override
  public void commit() throws IOException {
    Preconditions.checkState(mHopper != null, "commit() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot commit a transaction on an AtomicKijiPutter instance in state %s.", state);
    // We don't actually need the writer layout capsule here, but we want the layout update check.
    getWriterLayoutCapsule();

    List<Statement> statements = createCqlStatementsFromKeyValues(mTable, mEntityId, mHopper);
    assert(statements.size() > 0);

    //for (Statement s : statements) { mTable.getAdmin().getSession().execute(s); }

    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
    batchStatement.addAll(statements);

    // TODO: Possibly check that execution worked correctly.
    ResultSet resultSet = mTable.getAdmin().getSession().execute(batchStatement);
    LOG.info("Results from batch set: " + resultSet);

    reset();
  }

  /** {@inheritDoc} */
  @Override
  public <T> boolean checkAndCommit(String family, String qualifier, T value) throws IOException {
    Preconditions.checkState(mHopper != null,
        "checkAndCommit() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot checkAndCommit a transaction on an AtomicKijiPutter instance in state %s.", state);
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    //final HBaseColumnName columnName = capsule.getColumnNameTranslator().toHBaseColumnName(kijiColumnName);
    final byte[] encoded;

    // If passed value is null, then let encoded value be null.
    // HBase will check for non-existence of cell.
    if (null == value) {
      encoded = null;
    } else {
      final KijiCellEncoder cellEncoder =
          capsule.getCellEncoderProvider().getEncoder(family, qualifier);
      encoded = cellEncoder.encode(value);
    }

    // CQL currently supports only check-and-set operations that set the same cell that they
    // check.  See https://issues.apache.org/jira/browse/CASSANDRA-5633 for more details.  Thrift
    // may support everything that we need.

    // TODO: Possibly suport checkAndCommit if cell to check and cell to set are the same.

    throw new KijiIOException("Cassandra Kiji cannot yet support check-and-commit that inserts more than one cell.");
  }

  /**
   * Create a list of CQL Statements for inserting data into a table.
   *
   * @param table
   * @param entityId
   * @param keyValues
   * @return
   */
  private static List<Statement> createCqlStatementsFromKeyValues(
      CassandraKijiTable table,
      EntityId entityId,
      List<CassandraKeyValue> keyValues) {

    // Get the Cassandra table name for this column family
    String cassandraTableName = KijiManagedCassandraTableName.getKijiTableName(
        table.getURI(),
        table.getName()).toString();


    // TODO: Create this bound statement once, in the constructor.
    String queryString = String.format(
        "INSERT into %s (%s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?)",
        cassandraTableName,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL,
        CassandraKiji.CASSANDRA_VALUE_COL
    );
    LOG.info(queryString);

    PreparedStatement preparedStatement = table.getAdmin().getSession().prepare(queryString);

    ArrayList<Statement> statements = new ArrayList<Statement>();

    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    for (CassandraKeyValue kv : keyValues) {
      LOG.info("Binding timestamp " + kv.getTimestamp());
      statements.add(preparedStatement.bind(
          rowKey,
          kv.getLocalityGroup(),
          kv.getFamily(),
          kv.getQualifier(),
          kv.getTimestamp(),
          CassandraByteUtil.bytesToByteBuffer(kv.getValue())
      ));
    }

    return statements;
  }

  /** {@inheritDoc} */
  @Override
  public void rollback() {
    Preconditions.checkState(mHopper != null, "rollback() must be paired with a call to begin()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot rollback a transaction on an AtomicKijiPutter instance in state %s.", state);
    reset();
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, T value) throws IOException {
    //put(family, qualifier, HConstants.LATEST_TIMESTAMP, value);
    put(family, qualifier, System.currentTimeMillis(), value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(String family, String qualifier, long timestamp, T value) throws IOException {
    Preconditions.checkState(mHopper != null, "calls to put() must be between calls to begin() and "
        + "commit(), checkAndCommit(), or rollback()");
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to an AtomicKijiPutter instance in state %s.", state);
    final WriterLayoutCapsule capsule = getWriterLayoutCapsule();

    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);

    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    CassandraColumnNameTranslator translator = (CassandraColumnNameTranslator)capsule.getColumnNameTranslator();

    // Somewhat abusive here to use the HBase KeyValue to keep track of stuff to insert in a
    // Cassandra table, but KeyValue stores the family, qualifier, version, and value properly.
    mHopper.add(new CassandraKeyValue(
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName),
        translator.toCassandraColumnQualifier(kijiColumnName),
        timestamp,
        encoded));
  }

  /**
   * Get the writer layout capsule ensuring that the layout has not been updated while a transaction
   * is in progress.
   *
   * @return the WriterLayoutCapsule for this writer.
   * @throws org.kiji.schema.layout.LayoutUpdatedException in case the table layout has been updated while a transaction is
   * in progress
   */
  private WriterLayoutCapsule getWriterLayoutCapsule() throws LayoutUpdatedException {
    synchronized (mLock) {
      if (mLayoutOutOfDate) {
        // If the layout was updated, roll back the transaction and throw an Exception to indicate
        // the need to retry.
        rollback();
        // TODO: SCHEMA-468 improve error message for LayoutUpdatedException.
        throw new LayoutUpdatedException(
            "Table layout was updated during a transaction, please retry.");
      } else {
        return mWriterLayoutCapsule;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close an AtomicKijiPutter instance in state %s.", oldState);
    if (mHopper != null) {
      LOG.warn("Closing HBaseAtomicKijiPutter while a transaction on table {} on entity ID {} is "
          + "in progress. Rolling back transaction.", mTable.getURI(), mEntityId);
      reset();
    }
    mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
    mTable.release();
  }

  private class CassandraKeyValue {
    final String mLocalityGroup;
    final String mFamily;
    final String mQualifier;
    final Long mTimestamp;
    final byte[] mValue;

    CassandraKeyValue(String localityGroup, String family, String qualifier, Long timestamp, byte[] value){
      mFamily = family;
      mQualifier = qualifier;
      mTimestamp = timestamp;
      mLocalityGroup = localityGroup;
      mValue = value;
    }

    public String getLocalityGroup() { return mLocalityGroup; }
    public String getFamily() { return mFamily; }
    public String getQualifier() { return mQualifier; }
    public Long getTimestamp() { return mTimestamp; }
    public byte[] getValue() { return mValue; }
  }
}
