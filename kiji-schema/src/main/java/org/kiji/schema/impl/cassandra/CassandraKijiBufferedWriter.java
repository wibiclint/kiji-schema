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

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.util.StringUtils;
import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.*;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Cassandra implementation of a batch KijiTableWriter.
 *
 * For now, this implementation is less featured than the HBaseKijiBufferedWriter.  We choose when
 * to execute a series of writes not when the buffer reaches a certain size in raw bytes, but whether
 * when it reaches a certain size in the total number of puts (INSERT statements).
 *
 * We also do not combine puts to the same entity ID together into a single put.
 *
 * We arbitrarily choose to flush the write buffer when it contains 100 statements.
 *
 * Access to this Writer is threadsafe.  All internal state mutations must synchronize against
 * mInternalLock.
 */
@ApiAudience.Private
@Inheritance.Sealed
public class CassandraKijiBufferedWriter implements KijiBufferedWriter {
  // TODO: Improve performance by tracking what cluster nodes own what rows (based on partition key)
  // We can then bypass the client node and write directly to one of the data nodes.  The Cassandra
  // Hadoop output format does this already.
  // (The DataStax Java driver may already do this automatically by using token-aware routing.)

  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiBufferedWriter.class);

  /** KijiTable this writer is attached to. */
  private final CassandraKijiTable mTable;

  /** Session used for talking to this Cassandra table. */
  private final Session mSession;

  /** Object which processes layout update from the KijiTable to which this Writer writes. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /** Monitor against which all internal state mutations must be synchronized. */
  private final Object mInternalLock = new Object();

  private final PreparedStatement mPutStatement;

  /** Contains shared code with BufferedWriter. */
  private final CassandraKijiWriterCommon mWriterCommon;

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile CassandraKijiTableWriter.WriterLayoutCapsule mWriterLayoutCapsule = null;

  /** Local write buffers. */
  private ArrayList<Statement> mPutBuffer = Lists.newArrayList();
  private ArrayList<Statement> mDeleteBuffer = Lists.newArrayList();
  // Counter operations have to go into a separate Cassandra batch statement.
  private ArrayList<Statement> mCounterDeleteBuffer = Lists.newArrayList();

  /** Local write buffer size. */
  private long mMaxWriteBufferSize = 100L;
  private long mCurrentWriteBufferSize = 0L;

  /** States of a buffered writer instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /**
   * Tracks the state of this buffered writer.
   * Reads and writes to mState must by synchronized by mInternalLock.
   */
  private State mState = State.UNINITIALIZED;

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final LayoutCapsule capsule) throws IOException {
      synchronized (mInternalLock) {
        if (mState == State.CLOSED) {
          LOG.debug("BufferedWriter instance is closed; ignoring layout update.");
          return;
        }
        if (mState == State.OPEN) {
          LOG.info("Flushing buffer from HBaseKijiBufferedWriter for table: {} in preparation for"
              + " layout update.", mTable.getURI());
          flush();
        }

        final CellEncoderProvider provider = new CellEncoderProvider(
            mTable.getURI(),
            capsule.getLayout(),
            mTable.getKiji().getSchemaTable(),
            DefaultKijiCellEncoderFactory.get());
        // If the capsule is null this is the initial setup and we do not need a log message.
        if (mWriterLayoutCapsule != null) {
          LOG.debug(
              "Updating layout used by HBaseKijiBufferedWriter: "
              + "{} for table: {} from version: {} to: {}",
              this,
              mTable.getURI(),
              mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
              capsule.getLayout().getDesc().getLayoutId());
        } else {
          LOG.debug(
              "Initializing HBaseKijiBufferedWriter: {} for table: "
                  + "{} with table layout version: {}",
              this,
              mTable.getURI(),
              capsule.getLayout().getDesc().getLayoutId());
        }
        mWriterLayoutCapsule = new CassandraKijiTableWriter.WriterLayoutCapsule(
            provider,
            capsule.getLayout(),
            (CassandraColumnNameTranslator) capsule.getColumnNameTranslator());
      }
    }
  }

  /**
   * Creates a buffered kiji table writer that stores modifications to be sent on command
   * or when the buffer overflows.
   *
   * @param table A kiji table.
   * @throws org.kiji.schema.KijiTableNotFoundException in case of an invalid table parameter
   * @throws java.io.IOException in case of IO errors.
   */
  public CassandraKijiBufferedWriter(CassandraKijiTable table) throws IOException {
    mTable = table;
    mSession = mTable.getAdmin().getSession();
    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "CassandraKijiBufferedWriter for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only after everything else succeeded:
    mTable.retain();
    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.UNINITIALIZED,
          "Cannot open CassandraKijiBufferedWriter instance in state %s.", mState);
      mState = State.OPEN;
    }

    // TODO: Refactor this query text (and preparation for it) elsewhere.
    // Create the CQL statement to insert data.
    // Get a reference to the full name of the C* table for this column.
    // TODO: Refactor this name-creation code somewhere cleaner.
    KijiManagedCassandraTableName cTableName = KijiManagedCassandraTableName.getKijiTableName(
        mTable.getURI(),
        mTable.getName()
    );

    String queryText = String.format(
        "INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?);",
        cTableName,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL,
        CassandraKiji.CASSANDRA_VALUE_COL);

    mPutStatement = mSession.prepare(queryText);

    mWriterCommon = new CassandraKijiWriterCommon(mTable, mWriterLayoutCapsule);

  }

  // ----------------------------------------------------------------------------------------------
  // Puts

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    put(entityId, family, qualifier, System.currentTimeMillis(), value);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    // We cannot do a counter put within a buffered writer, because doing a counter put in
    // Cassandra requires doing a read to get the current counter value, followed by an increment.
    if (mWriterCommon.isCounterColumn(family, qualifier)) {
      // TODO: Better error message.
      throw new UnsupportedOperationException("Cannot perform a counter set with a buffered writer.");
    }

    Statement putStatement =
        mWriterCommon.getStatementPutNotCounter(entityId, family, qualifier, timestamp, value);

    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot write to BufferedWriter instance in state %s.", mState);

      mPutBuffer.add(putStatement);
      // TODO: Figure out how much space in the buffer this put actually takes.
      mCurrentWriteBufferSize += 1;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /**
   * Add a Delete to the buffer and update the current buffer size.
   *
   * @param statement A delete to add to the buffer.
   * @throws java.io.IOException in case of an error on flush.
   */
  private void updateBufferWithDelete(Statement statement) throws IOException {
    // TODO: Figure out how big a delete actually is.
    synchronized (mInternalLock) {
      mDeleteBuffer.add(statement);
      mCurrentWriteBufferSize += 1;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  private void updateBufferWithCounterDelete(Statement statement) throws IOException {
    // TODO: Figure out how big a delete actually is.
    synchronized (mInternalLock) {
      mCounterDeleteBuffer.add(statement);
      mCurrentWriteBufferSize += 1;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    updateBufferWithDelete(mWriterCommon.getStatementDeleteRow(entityId));
    updateBufferWithCounterDelete(mWriterCommon.getStatementDeleteRowCounter(entityId));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    throw new UnsupportedOperationException(
        "Cannot delete with an up-to timestamp in Cassandra Kiji"
    );
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family) throws IOException {
    updateBufferWithDelete(mWriterCommon.getStatementDeleteFamily(entityId, family));
    updateBufferWithCounterDelete(mWriterCommon.getStatementDeleteFamilyCounter(entityId, family));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {
    throw new UnsupportedOperationException(
        "Cannot delete with an up-to timestamp in Cassandra Kiji"
    );
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier) throws IOException {
    Statement statement = mWriterCommon.getStatementDeleteColumn(entityId, family, qualifier);
    if (mWriterCommon.isCounterColumn(family, qualifier)) {
      updateBufferWithCounterDelete(statement);
    } else {
      updateBufferWithDelete(statement);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    throw new UnsupportedOperationException(
        "Cannot delete with an up-to timestamp in Cassandra Kiji"
    );
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier) throws IOException {
    // Okay if this is a counter, because we know what the timestamp is.
    if (mWriterCommon.isCounterColumn(family, qualifier)) {
      Statement statement = mWriterCommon.getStatementDeleteCell(
          entityId, family, qualifier, KConstants.CASSANDRA_COUNTER_TIMESTAMP);
      updateBufferWithCounterDelete(statement);
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
    Statement statement = mWriterCommon.getStatementDeleteCell(entityId, family, qualifier, timestamp);
    if (mWriterCommon.isCounterColumn(family, qualifier)) {
      updateBufferWithCounterDelete(statement);
    } else {
      updateBufferWithDelete(statement);
    }
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void setBufferSize(long bufferSize) throws IOException {
    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot set buffer size of BufferedWriter instance %s in state %s.", this, mState);
      Preconditions.checkArgument(bufferSize > 0,
          "Buffer size cannot be negative, got %s.", bufferSize);
      mMaxWriteBufferSize = bufferSize;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    // This looks a little bit fishy that we do all of the deletes and then all of the writes.
    // Seems like there could be some event-ordering problems.
    // TODO: Check for potential delete/put event-ordering issues in this implementation.
    // Possibly put everything in to one big put/delete combined queue.

    LOG.info("Flushing CassandraKijiBufferedWriter.");

    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot flush BufferedWriter instance %s in state %s.", this, mState);
      if (mDeleteBuffer.size() > 0) {
        LOG.info("Delete buffer has " + mDeleteBuffer.size() + " entries.");
        BatchStatement deleteStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        deleteStatement.addAll(mDeleteBuffer);
        mSession.execute(deleteStatement);
        mDeleteBuffer.clear();
      }
      if (mCounterDeleteBuffer.size() > 0) {
        LOG.info("Counter delete buffer has " + mCounterDeleteBuffer.size() + " entries.");
        BatchStatement deleteStatement = new BatchStatement(BatchStatement.Type.COUNTER);
        deleteStatement.addAll(mCounterDeleteBuffer);
        mSession.execute(deleteStatement);
        mCounterDeleteBuffer.clear();
      }
      if (mPutBuffer.size() > 0) {
        BatchStatement putStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        putStatement.addAll(mPutBuffer);
        mSession.execute(putStatement);
        mPutBuffer.clear();
      }
      mCurrentWriteBufferSize = 0L;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    synchronized (mInternalLock) {
      flush();
      Preconditions.checkState(mState == State.OPEN,
          "Cannot close BufferedWriter instance %s in state %s.", this, mState);
      mState = State.CLOSED;
      mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
      mTable.release();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    try {
      if (mState != State.CLOSED) {
        LOG.warn("Finalizing unclosed HBaseKijiBufferedWriter {} in state {}.", this, mState);
        close();
      }
    } catch (Throwable thr) {
      LOG.warn("Throwable thrown by close() in finalize of HBaseKijiBufferedWriter: {}\n{}",
          thr.getMessage(), StringUtils.stringifyException(thr));
    } finally {
      super.finalize();
    }
  }
}
