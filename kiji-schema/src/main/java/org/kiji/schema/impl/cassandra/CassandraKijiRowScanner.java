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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Preconditions;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Iterators;
import org.apache.hadoop.hbase.util.Bytes;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.util.Debug;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The internal implementation of KijiRowScanner that reads from C* tables.
 */
@ApiAudience.Private
public class CassandraKijiRowScanner implements KijiRowScanner {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiRowScanner.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + CassandraKijiRowScanner.class.getName());

  /** The request used to fetch the row data. */
  private final KijiDataRequest mDataRequest;

  /** The table being scanned. */
  private final CassandraKijiTable mTable;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mCellDecoderProvider;

  /** States of a row scanner instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this row scanner. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Factory for entity IDs. */
  private final EntityIdFactory mEntityIdFactory;

  /** Cassandra Admin object (used for executing CQL commands). */
  private final CassandraAdmin mAdmin;

  /** For debugging finalize(). */
  private String mConstructorStack = "";

  /** List of results returned from C* queries. */
  private List<PeekingIterator<Row>> mRowIterators;

  KijiRowData mNextRow;

  /**
   *
   * @param table
   * @param dataRequest
   * @param cellDecoderProvider
   * @param resultSets
   * @throws IOException
   */
  public CassandraKijiRowScanner(
      CassandraKijiTable table,
      KijiDataRequest dataRequest,
      CellDecoderProvider cellDecoderProvider,
      List<ResultSet> resultSets
    ) throws IOException {

    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }

    mDataRequest = dataRequest;
    mTable = table;
    mAdmin = mTable.getAdmin();
    mCellDecoderProvider = cellDecoderProvider;
    mEntityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiRowScanner instance in state %s.", oldState);

    // Create the row iterators from the C* results.
    mRowIterators = new ArrayList<PeekingIterator<Row>>();

    for (ResultSet resultSet : resultSets) {
      Iterator<Row> rowIterator = resultSet.iterator();
      mRowIterators.add(Iterators.peekingIterator(rowIterator));
    }

    mNextRow = getNextRow();
  }

  /** {@inheritDoc} */
  @Override
  public CassandraKijiRowIterator iterator() {
    return new CassandraKijiRowIterator();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiRowScanner instance in state %s.", oldState);
    //mResultScanner.close();
    //mHTable.close();
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      CLEANUP_LOG.warn(
          "Finalizing unclosed KijiRowScanner in state {}.\n"
          + "Call stack when the scanner was constructed:\n{}",
          state,
          mConstructorStack);
      close();
    }
    super.finalize();
  }

  /**
   * Look at all of the iterators over Cassandra Rows and return the entity ID (Cassandra partition
   * key) that has the lowest token value.
   *
   * @return The EntityId for the Cassandra partition key with the lowest Cassandra token value, or
   *     null if there are no more rows left to read.
   */
  private ByteBuffer getEntityIdWithLowestTokenValue() {
    ByteBuffer lowestTokenEntityId = null;
    Long lowestTokenValue = null;

    for (PeekingIterator<Row> iterator : mRowIterators) {
      Row row = iterator.peek();

      // No more data for this iterator.
      if (row == null) {
        continue;
      }

      Long tokenValue = row.getLong(String.format("token(%s)", CassandraKiji.CASSANDRA_KEY_COL));
      ByteBuffer entityIdBlob = row.getBytes(CassandraKiji.CASSANDRA_KEY_COL);

      if (null == lowestTokenValue || tokenValue < lowestTokenValue) {
        lowestTokenValue = tokenValue;
        lowestTokenEntityId = entityIdBlob;
      }
    }
    return lowestTokenEntityId;
  }

  /**
   * Get the a KijiRowData for the next row.  If we are out of data, then return null.
   * @return The next KijiRowData for this scanner, or null if we are out of data.
   */
  private KijiRowData getNextRow() {
    // TODO: Keep a set of already-seen entity IDs to make sure that we don't hit one twice (sanity check).

    LOG.info("Getting next row for CassandraKijiRowScanner.");

    // In this section of the code, we create a new KijiRowData instance by looking at the Cassandra
    // row data at the head of all of the various Row iterators that we have.  Creating the
    // KijiRowData is tricky only because every Cassandra Row object may not contain an entry for
    // every Kiji entity ID.  We can use the backing Cassandra table's "token" function to order the
    // Cassandra Rows by partition key (Cassandra should always return Rows to use in the order
    // dictated by the token function of the partition keys).

    // The CassandraDataRequestAdapter adds token(key) to the query that creates the Cassandra Row
    // data that we deal with in this method.

    // Therefore, to create a KijiRowData, we get the entity ID with the lowest token value from all
    // of our iterators over Cassandra Rows, get all of the data for the rows with that
    // lowest-token-value entity ID, and then stitch them together.

    ByteBuffer entityIdWithLowestToken = getEntityIdWithLowestTokenValue();

    if (null == entityIdWithLowestToken) {
      LOG.info("No more data, returning null for next value.");
      return null;
    }

    // Get a big set of Row objects for the given entity ID.
    HashSet<Row> rowsThisEntityId = new HashSet<Row>();

    LOG.info("Still data left for another row!");
    assert (null != entityIdWithLowestToken);

    for (PeekingIterator<Row> iterator : mRowIterators) {

      // Add all of the rows for this iterator until the entity ID changes.
      while (
          iterator.peek() != null &&
          entityIdWithLowestToken.equals(iterator.peek().getBytes(CassandraKiji.CASSANDRA_KEY_COL))
      ) {
        Row row = iterator.next();
        // If this assertion fails, something is screwy with the peeking iterator.
        assert(row.getBytes(CassandraKiji.CASSANDRA_KEY_COL).equals(entityIdWithLowestToken));
        rowsThisEntityId.add(row);
      }
    }

    // Actually create the entity ID from the ByteBuffer.
    byte[] eidBytes = CassandraByteUtil.byteBuffertoBytes(entityIdWithLowestToken);
    EntityId eid = mEntityIdFactory.getEntityIdFromHBaseRowKey(eidBytes);

    // Now create a KijiRowData with all of these rows.
    try {
      return new CassandraKijiRowData(mTable, mDataRequest, eid, rowsThisEntityId, mCellDecoderProvider);
    } catch (IOException ioe) {
      // TODO: I'm not sure how to handle an exception here...
      System.err.println("Error creating KijiRowData");
      return null;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Wraps a Kiji row scanner into a Java iterator. */
  private class CassandraKijiRowIterator implements Iterator<KijiRowData> {
    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      final State state = mState.get();
      Preconditions.checkState(state == State.OPEN,
          "Cannot check has next on KijiRowScanner instance in state %s.", state);
      return (mNextRow != null);
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData next() {
      final State state = mState.get();
      Preconditions.checkState(state == State.OPEN,
          "Cannot get next on KijiRowScanner instance in state %s.", state);
      if (mNextRow == null) {
        // Comply with the Iterator interface:
        throw new NoSuchElementException();
      }
      final KijiRowData data = mNextRow;
      mNextRow = getNextRow();
      return data;
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("KijiRowIterator does not support remove().");
    }
  }
}
