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
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.util.StringUtils;
import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.*;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.hbase.HBaseKijiTable;
import org.kiji.schema.impl.hbase.HBaseKijiTableWriter.WriterLayoutCapsule;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.platform.SchemaPlatformBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /** Local write buffers. */
  private ArrayList<Statement> mPutBuffer = Lists.newArrayList();
  private ArrayList<Statement> mDeleteBuffer = Lists.newArrayList();

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
        mWriterLayoutCapsule = new WriterLayoutCapsule(
            provider,
            capsule.getLayout(),
            capsule.getColumnNameTranslator());
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
        "INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?);",
        cTableName,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL,
        CassandraKiji.CASSANDRA_VALUE_COL);

    mPutStatement = mSession.prepare(queryText);

  }

  // ----------------------------------------------------------------------------------------------
  // Puts

  /**
   * Add a Put to the buffer and update the current buffer size.
   *
   * @param entityId the EntityId of the row to put into.
   * @param putStatement bound statement containing the insertion.
   * @throws java.io.IOException in case of an error on flush.
   */
  private void updateBufferWithPut(EntityId entityId, Statement putStatement) throws IOException {
    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot write to BufferedWriter instance in state %s.", mState);

      mPutBuffer.add(putStatement);
      mCurrentWriteBufferSize += 1;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
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
    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;

    // TODO: Column name translation.

    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);
    final byte[] encoded = cellEncoder.encode(value);
    final ByteBuffer encodedByteBuffer = CassandraByteUtil.bytesToByteBuffer(encoded);

    // Encode the EntityId as a ByteBuffer for C*.
    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    BoundStatement boundStatement = mPutStatement.bind(
        rowKey,
        family,
        qualifier,
        timestamp,
        encodedByteBuffer);

    updateBufferWithPut(entityId, boundStatement);
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
    synchronized (mInternalLock) {
      mDeleteBuffer.add(statement);
      mCurrentWriteBufferSize += 1;
      if (mCurrentWriteBufferSize > mMaxWriteBufferSize) {
        flush();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    deleteRow(entityId, HConstants.LATEST_TIMESTAMP);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    //final Delete delete = new Delete(entityId.getHBaseRowKey(), upToTimestamp, null);
    //updateBuffer(delete);
    // TODO: Add support for deletes.
    throw new UnsupportedOperationException("Haven't written this code yet!");
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
    throw new UnsupportedOperationException("Haven't written this code yet!");
    /*
    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final FamilyLayout familyLayout = capsule.getLayout().getFamilyMap().get(family);
    if (null == familyLayout) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }

    if (familyLayout.getLocalityGroup().getFamilyMap().size() > 1) {
      // There are multiple families within the locality group, so we need to be clever.
      if (familyLayout.isGroupType()) {
        deleteGroupFamily(entityId, familyLayout, upToTimestamp);
      } else if (familyLayout.isMapType()) {
        deleteMapFamily(entityId, familyLayout, upToTimestamp);
      } else {
        throw new RuntimeException("Internal error: family is neither map-type nor group-type.");
      }
      return;
    }

    // The only data in this HBase family is the one Kiji family, so we can delete everything.
    final HBaseColumnName hbaseColumnName = capsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(family));
    final Delete delete = new Delete(entityId.getHBaseRowKey());
    delete.deleteFamily(hbaseColumnName.getFamily(), upToTimestamp);

    // Buffer the delete.
    updateBuffer(delete);
    */
  }

  /**
   * Deletes all cells from a group-type family with a timestamp less than or equal to a
   * specified timestamp.
   *
   * @param entityId The entity (row) to delete from.
   * @param familyLayout The family layout.
   * @param upToTimestamp A timestamp.
   * @throws java.io.IOException If there is an IO error.
   */
  private void deleteGroupFamily(
      EntityId entityId,
      FamilyLayout familyLayout,
      long upToTimestamp)
      throws IOException {
    throw new UnsupportedOperationException("Haven't written this code yet!");
    /*
    final String familyName = Preconditions.checkNotNull(familyLayout.getName());
    // Delete each column in the group according to the layout.
    final Delete delete = new Delete(entityId.getHBaseRowKey());
    for (ColumnLayout columnLayout : familyLayout.getColumnMap().values()) {
      final String qualifier = columnLayout.getName();
      final KijiColumnName column = new KijiColumnName(familyName, qualifier);
      final HBaseColumnName hbaseColumnName =
          mWriterLayoutCapsule.getColumnNameTranslator().toHBaseColumnName(column);
      delete.deleteColumns(
          hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
    }

    // Buffer the delete.
    updateBuffer(delete);
    */
  }

  /**
   * Deletes all cells from a map-type family with a timestamp less than or equal to a
   * specified timestamp.
   *
   * <p>This call requires an HBase row lock, so it should be used with care.</p>
   *
   * @param entityId The entity (row) to delete from.
   * @param familyLayout A family layout.
   * @param upToTimestamp A timestamp.
   * @throws java.io.IOException If there is an IO error.
   */
  private void deleteMapFamily(EntityId entityId, FamilyLayout familyLayout, long upToTimestamp)
      throws IOException {
    // Since multiple Kiji column families are mapped into a single HBase column family,
    // we have to do this delete in a two-step transaction:
    //
    // 1. Send a get() to retrieve the names of all HBase qualifiers within the HBase
    //    family that belong to the Kiji column family.
    // 2. Send a delete() for each of the HBase qualifiers found in the previous step.
    throw new UnsupportedOperationException("Haven't written this code yet!");

    /*
    final String familyName = familyLayout.getName();
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(familyName));
    final byte[] hbaseRow = entityId.getHBaseRowKey();

    // Lock the row.
    final RowLock rowLock = mHTable.lockRow(hbaseRow);
    try {
      // Step 1.
      final Get get = new Get(hbaseRow, rowLock);
      get.addFamily(hbaseColumnName.getFamily());

      final FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filter.addFilter(new KeyOnlyFilter());
      filter.addFilter(new ColumnPrefixFilter(hbaseColumnName.getQualifier()));
      get.setFilter(filter);

      final Result result = mHTable.get(get);

      // Step 2.
      if (result.isEmpty()) {
        LOG.debug("No qualifiers to delete in map family: " + familyName);
      } else {
        final Delete delete = new Delete(hbaseRow, HConstants.LATEST_TIMESTAMP, rowLock);
        for (byte[] hbaseQualifier
                 : result.getFamilyMap(hbaseColumnName.getFamily()).keySet()) {
          LOG.debug("Deleting HBase column " + hbaseColumnName.getFamilyAsString()
              + ":" + Bytes.toString(hbaseQualifier));
          delete.deleteColumns(hbaseColumnName.getFamily(), hbaseQualifier, upToTimestamp);
        }
        updateBuffer(delete);
      }
    } finally {
      // Make sure to unlock the row!
      mHTable.unlockRow(rowLock);
    }
    */
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
    throw new UnsupportedOperationException("Haven't written this code yet!");
    /*
    final HBaseColumnName hbaseColumnName = mWriterLayoutCapsule.getColumnNameTranslator()
        .toHBaseColumnName(new KijiColumnName(family, qualifier));
    final Delete delete = new Delete(entityId.getHBaseRowKey())
        .deleteColumns(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), upToTimestamp);
    updateBuffer(delete);
    */
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
    throw new UnsupportedOperationException("Haven't written this code yet!");
    /*
    final HBaseColumnName hbaseColumnName =
        mTable.getColumnNameTranslator().toHBaseColumnName(new KijiColumnName(family, qualifier));
    final Delete delete = new Delete(entityId.getHBaseRowKey())
        .deleteColumn(hbaseColumnName.getFamily(), hbaseColumnName.getQualifier(), timestamp);
    updateBuffer(delete);
    */
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
    synchronized (mInternalLock) {
      Preconditions.checkState(mState == State.OPEN,
          "Cannot flush BufferedWriter instance %s in state %s.", this, mState);
      if (mDeleteBuffer.size() > 0) {
        BatchStatement deleteStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        deleteStatement.addAll(mDeleteBuffer);
        mSession.execute(deleteStatement);
        mDeleteBuffer.clear();
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
