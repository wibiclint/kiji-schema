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

  /** The KijiRowIterator backing this scanner. */
  //private final KijiRowIterator kijiRowIterator;


  // -----------------------------------------------------------------------------------------------

  /**
   * A class to encapsulate the various options the CassandraKijiRowScanner constructor requires.
   */
  public static class Options {
    private KijiDataRequest mDataRequest;
    private CassandraKijiTable mTable;
    private CellDecoderProvider mCellDecoderProvider;

    /**
     * Sets the data request used to generate the KijiRowScanner.
     *
     * @param dataRequest A data request.
     * @return This options instance.
     */
    public Options withDataRequest(KijiDataRequest dataRequest) {
      mDataRequest = dataRequest;
      return this;
    }

    /**
     * Sets the table being scanned.
     *
     * @param table The table being scanned.
     * @return This options instance.
     */
    public Options withTable(CassandraKijiTable table) {
      mTable = table;
      return this;
    }

    /**
     * Sets a provider for cell decoders.
     *
     * @param cellDecoderProvider Provider for cell decoders.
     * @return This options instance.
     */
    public Options withCellDecoderProvider(CellDecoderProvider cellDecoderProvider) {
      mCellDecoderProvider = cellDecoderProvider;
      return this;
    }

    /**
     * Gets the data request.
     *
     * @return The data request.
     */
    public KijiDataRequest getDataRequest() {
      return mDataRequest;
    }

    /**
     * Gets the table being scanned.
     *
     * @return The Kiji table.
     */
    public CassandraKijiTable getTable() {
      return mTable;
    }

    /**
     * Gets the provider for cell decoders.
     *
     * @return the provider for cell decoders.
     */
    public CellDecoderProvider getCellDecoderProvider() {
      return mCellDecoderProvider;
    }

  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Creates a new <code>KijiRowScanner</code> instance.
   *
   * @param options The options for this scanner.
   * @throws java.io.IOException on I/O error.
   */
  public CassandraKijiRowScanner(Options options) throws IOException {
    if (CLEANUP_LOG.isDebugEnabled()) {
      mConstructorStack = Debug.getStackTrace();
    }

    mDataRequest = options.getDataRequest();
    mTable = options.getTable();
    mAdmin = mTable.getAdmin();

    mCellDecoderProvider = options.getCellDecoderProvider();
    mEntityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiRowScanner instance in state %s.", oldState);

    // Turn the data request into a bunch of Cassandra queries.
    // Get back a set of ResultSet objects, one per C* query.
    //List<ResultSet> resultSets = queryCassandraTables();

    // Use the ResultSet objects to create a KijiRowIterator.
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowIterator iterator() {
    return new KijiRowIterator();
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

  // -----------------------------------------------------------------------------------------------

  /** Wraps a Kiji row scanner into a Java iterator. */
  // TODO: Implement this class!
  private class KijiRowIterator implements Iterator<KijiRowData> {
    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      final State state = mState.get();
      Preconditions.checkState(state == State.OPEN,
          "Cannot check has next on KijiRowScanner instance in state %s.", state);
      //return (mNextResult != null);
      return false;
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData next() {
      /*
      final State state = mState.get();
      Preconditions.checkState(state == State.OPEN,
          "Cannot get next on KijiRowScanner instance in state %s.", state);
      if (mNextResult == null) {
        // Comply with the Iterator interface:
        throw new NoSuchElementException();
      }
      final Result result = mNextResult;
      mLastReturnedKey = result.getRow();

      // Prefetch the next row for hasNext():
      mNextResult = getNextResult();

      // Decode the Cassandra result into a KijiRowData:
      try {
        final EntityId entityId = mEntityIdFactory.getEntityIdFromCassandraRowKey(result.getRow());
        return new CassandraKijiRowData(mTable, mDataRequest, entityId, result, mCellDecoderProvider);
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
      */
      return null;
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("KijiRowIterator does not support remove().");
    }
  }
}
