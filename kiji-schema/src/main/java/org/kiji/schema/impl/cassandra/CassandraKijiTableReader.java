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

import com.datastax.driver.core.*;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.KijiTableReaderBuilder.OnDecoderCacheMiss;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.filter.KijiRowFilterApplicator;
import org.kiji.schema.hbase.HBaseScanOptions;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reads from a kiji table by sending the requests directly to the C* tables.
 */
@ApiAudience.Private
public final class CassandraKijiTableReader implements KijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTableReader.class);

  /** C* KijiTable to read from. */
  private final CassandraKijiTable mTable;

  /** Behavior when a cell decoder cannot be found. */
  private final OnDecoderCacheMiss mOnDecoderCacheMiss;

  /** States of a kiji table reader instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this KijiTableReader instance. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Map of overridden CellSpecs to use when reading. Null when mOverrides is not null. */
  private final Map<KijiColumnName, CellSpec> mCellSpecOverrides;

  /** Map of overridden column read specifications. Null when mCellSpecOverrides is not null. */
  private final Map<KijiColumnName, BoundColumnReaderSpec> mOverrides;

  /** Map of backup column read specifications. Null when mCellSpecOverrides is not null. */
  private final Collection<BoundColumnReaderSpec> mAlternatives;

  /** Object which processes layout update from the KijiTable from which this Reader reads. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /**
   * Encapsulation of all table layout related state necessary for the operation of this reader.
   * Can be hot swapped to reflect a table layout update.
   */
  private ReaderLayoutCapsule mReaderLayoutCapsule = null;

  /**
   * Container class encapsulating all reader state which must be updated in response to a table
   * layout update.
   */
  private static final class ReaderLayoutCapsule {
    private final CellDecoderProvider mCellDecoderProvider;
    private final KijiTableLayout mLayout;
    private final ColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellDecoderProvider the CellDecoderProvider to cache.  This provider should reflect
     *     all overrides appropriate to this reader.
     * @param layout the KijiTableLayout to cache.
     * @param translator the ColumnNameTranslator to cache.
     */
    private ReaderLayoutCapsule(
        final CellDecoderProvider cellDecoderProvider,
        final KijiTableLayout layout,
        final ColumnNameTranslator translator) {
      mCellDecoderProvider = cellDecoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the column name translator for the current layout.
     * @return the column name translator for the current layout.
     */
    private ColumnNameTranslator getColumnNameTranslator() {
      return mTranslator;
    }

    /**
     * Get the current table layout for the table to which this reader is associated.
     * @return the current table layout for the table to which this reader is associated.
     */
    private KijiTableLayout getLayout() {
      return mLayout;
    }

    /**
     * Get the CellDecoderProvider including CellSpec overrides for providing cell decoders for the
     * current layout.
     * @return the CellDecoderProvider including CellSpec overrides for providing cell decoders for
     * the current layout.
     */
    private CellDecoderProvider getCellDecoderProvider() {
      return mCellDecoderProvider;
    }
  }

  /** Provides for the updating of this Reader in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(LayoutCapsule capsule) throws IOException {
      final CellDecoderProvider provider;
      if (null != mCellSpecOverrides) {
        provider = new CellDecoderProvider(
            capsule.getLayout(),
            mTable.getKiji().getSchemaTable(),
            SpecificCellDecoderFactory.get(),
            mCellSpecOverrides);
      } else {
        provider = new CellDecoderProvider(
            capsule.getLayout(),
            mOverrides,
            mAlternatives,
            mOnDecoderCacheMiss);
      }
      if (mReaderLayoutCapsule != null) {
        LOG.debug(
            "Updating layout used by KijiTableReader: {} for table: {} from version: {} to: {}",
            this,
            mTable.getURI(),
            mReaderLayoutCapsule.getLayout().getDesc().getLayoutId(),
            capsule.getLayout().getDesc().getLayoutId());
      } else {
        // If the capsule is null this is the initial setup and we need a different log message.
        LOG.debug("Initializing KijiTableReader: {} for table: {} with table layout version: {}",
            this,
            mTable.getURI(),
            capsule.getLayout().getDesc().getLayoutId());
      }
      mReaderLayoutCapsule = new ReaderLayoutCapsule(
          provider,
          capsule.getLayout(),
          capsule.getColumnNameTranslator());
    }
  }

  /**
   * Creates a new <code>CassandraKijiTableReader</code> instance that sends the read requests
   * directly to Cassandra.
   *
   * @param table Kiji table from which to read.
   * @throws java.io.IOException on I/O error.
   * @return a new CassandraKijiTableReader.
   */
  public static CassandraKijiTableReader create(
      final CassandraKijiTable table
  ) throws IOException {
    return CassandraKijiTableReaderBuilder.create(table).buildAndOpen();
  }

  /**
   * Creates a new <code>HbaseKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param overrides layout overrides to modify read behavior.
   * @return a new CassandraKijiTableReader.
   * @throws java.io.IOException in case of an error opening the reader.
   */
  public static CassandraKijiTableReader createWithCellSpecOverrides(
      final CassandraKijiTable table,
      final Map<KijiColumnName, CellSpec> overrides
  ) throws IOException {
    return new CassandraKijiTableReader(table, overrides);
  }

  /**
   * Create a new <code>CassandraKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param onDecoderCacheMiss behavior to use when a {@link org.kiji.schema.layout.ColumnReaderSpec} override
   *     specified in a {@link org.kiji.schema.KijiDataRequest} cannot be found in the prebuilt cache of cell
   *     decoders.
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     KijiTableReader will accept as overrides in data requests.
   * @return a new CassandraKijiTableReader.
   * @throws java.io.IOException in case of an error opening the reader.
   */
  public static CassandraKijiTableReader createWithOptions(
      final CassandraKijiTable table,
      final OnDecoderCacheMiss onDecoderCacheMiss,
      final Map<KijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<KijiColumnName, ColumnReaderSpec> alternatives
  ) throws IOException {
    return new CassandraKijiTableReader(table, onDecoderCacheMiss, overrides, alternatives);
  }

  /**
   * Open a table reader whose behavior is customized by overriding CellSpecs.
   *
   * @param table Kiji table from which this reader will read.
   * @param cellSpecOverrides specifications of overriding read behaviors.
   * @throws java.io.IOException in case of an error opening the reader.
   */
  private CassandraKijiTableReader(
      final CassandraKijiTable table,
      final Map<KijiColumnName, CellSpec> cellSpecOverrides
  ) throws IOException {
    mTable = table;
    mCellSpecOverrides = cellSpecOverrides;
    mOnDecoderCacheMiss = KijiTableReaderBuilder.DEFAULT_CACHE_MISS;
    mOverrides = null;
    mAlternatives = null;

    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mReaderLayoutCapsule != null,
        "KijiTableReader for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableReader instance in state %s.", oldState);
  }

  /**
   * Creates a new <code>CassandraKijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Kiji table from which to read.
   * @param onDecoderCacheMiss behavior to use when a {@link org.kiji.schema.layout.ColumnReaderSpec} override
   *     specified in a {@link org.kiji.schema.KijiDataRequest} cannot be found in the prebuilt cache of cell
   *     decoders.
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     KijiTableReader will accept as overrides in data requests.
   * @throws java.io.IOException on I/O error.
   */
  private CassandraKijiTableReader(
      final CassandraKijiTable table,
      final OnDecoderCacheMiss onDecoderCacheMiss,
      final Map<KijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<KijiColumnName, ColumnReaderSpec> alternatives
  ) throws IOException {
    mTable = table;
    mOnDecoderCacheMiss = onDecoderCacheMiss;

    final KijiTableLayout layout = mTable.getLayout();
    final Set<KijiColumnName> layoutColumns = layout.getColumnNames();
    final Map<KijiColumnName, BoundColumnReaderSpec> boundOverrides = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, ColumnReaderSpec> override
        : overrides.entrySet()) {
      final KijiColumnName column = override.getKey();
      if (!layoutColumns.contains(column)
          && !layoutColumns.contains(new KijiColumnName(column.getFamily()))) {
        throw new NoSuchColumnException(String.format(
            "KijiTableLayout: %s does not contain column: %s", layout, column));
      } else {
        boundOverrides.put(column,
            BoundColumnReaderSpec.create(override.getValue(), column));
      }
    }
    mOverrides = boundOverrides;
    final Collection<BoundColumnReaderSpec> boundAlternatives = Sets.newHashSet();
    for (Map.Entry<KijiColumnName, ColumnReaderSpec> altsEntry
        : alternatives.entries()) {
      final KijiColumnName column = altsEntry.getKey();
      if (!layoutColumns.contains(column)
          && !layoutColumns.contains(new KijiColumnName(column.getFamily()))) {
        throw new NoSuchColumnException(String.format(
            "KijiTableLayout: %s does not contain column: %s", layout, column));
      } else {
        boundAlternatives.add(
            BoundColumnReaderSpec.create(altsEntry.getValue(), altsEntry.getKey()));
      }
    }
    mAlternatives = boundAlternatives;
    mCellSpecOverrides = null;

    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mReaderLayoutCapsule != null,
        "KijiTableReader for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableReader instance in state %s.", oldState);
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(EntityId entityId, KijiDataRequest dataRequest)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get row from KijiTableReader instance %s in state %s.", this, state);

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    // Make sure the request validates against the layout of the table.
    final KijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);

    // TODO: Insert column-name translator here.
    CassandraDataRequestAdapter adapter = new CassandraDataRequestAdapter(dataRequest, null);

    List<ResultSet> results = adapter.doGet(mTable, entityId, tableLayout);

    HashSet<Row> allRows = new HashSet<Row>();

    for (ResultSet res : results) {
      for (Row row : res.all()) {
        allRows.add(row);
      }
    }

    // Now we create a KijiRowData from all of these results.
    // Parse the result.
    return new CassandraKijiRowData(
        mTable, dataRequest, entityId, allRows, capsule.getCellDecoderProvider());
  }

  // TODO: Clean this up, possibly refactor.
  /**
   * Necessary for KijiMR in record reader to get KijiRowData from a Cassandra Row.
   *
   * @param dataRequest
   * @param row
   * @return
   * @throws IOException
   */
  public KijiRowData getRowDataFromCassandraRow(KijiDataRequest dataRequest, Row row) throws IOException {
    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    // Make sure the request validates against the layout of the table.
    final KijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);

    // TODO: Insert column-name translator here.

    HashSet<Row> allRows = new HashSet<Row>();
    allRows.add(row);

    // Figure out the entity ID from the row.
    ByteBuffer currentEntityIdBlob = row.getBytes(CassandraKiji.CASSANDRA_KEY_COL);
    byte[] eidBytes = CassandraByteUtil.byteBuffertoBytes(currentEntityIdBlob);
    EntityIdFactory entityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());
    EntityId entityId = entityIdFactory.getEntityIdFromHBaseRowKey(eidBytes);

    // Now we create a KijiRowData from all of these results.
    // Parse the result.
    return new CassandraKijiRowData(
        mTable, dataRequest, entityId, allRows, capsule.getCellDecoderProvider());
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest)
      throws IOException {
    /*
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get rows from KijiTableReader instance %s in state %s.", this, state);

    // Bulk gets have some overhead associated with them,
    // so delegate work to get(EntityId, KijiDataRequest) if possible.
    if (entityIds.size() == 1) {
      return Collections.singletonList(this.get(entityIds.get(0), dataRequest));
    }
    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    final KijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);
    final HBaseDataRequestAdapter hbaseRequestAdapter =
        new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());

    // Construct a list of hbase Gets to send to the HTable.
    final List<Get> hbaseGetList = makeGetList(entityIds, tableLayout, hbaseRequestAdapter);

    // Send the HTable Gets.
    final Result[] results = doHBaseGet(hbaseGetList);
    Preconditions.checkState(entityIds.size() == results.length);

    // Parse the results.  If a Result is null, then the corresponding KijiRowData should also
    // be null.  This indicates that there was an error retrieving this row.
    List<KijiRowData> rowDataList = parseResults(results, entityIds, dataRequest, tableLayout);

    return rowDataList;
    */
    // TODO: Implement in C*.
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(KijiDataRequest dataRequest) throws IOException {
    return getScanner(dataRequest, new KijiScannerOptions());
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      KijiDataRequest dataRequest,
      KijiScannerOptions kijiScannerOptions)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get scanner from KijiTableReader instance %s in state %s.", this, state);

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    // Make sure the request validates against the layout of the table.
    final KijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);

    // TODO: Insert column-name translator here.
    CassandraDataRequestAdapter adapter = new CassandraDataRequestAdapter(dataRequest, null);

    List<ResultSet> results = adapter.doScan(mTable, tableLayout, kijiScannerOptions);

    // Now we create a KijiRowData from all of these results.
    // Parse the result.
    return new CassandraKijiRowScanner(mTable, dataRequest, capsule.getCellDecoderProvider(), results);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraKijiTableReader.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .add("layout-version", mReaderLayoutCapsule.getLayout().getDesc().getLayoutId())
        .add("state", mState.get())
        .toString();
  }

  /**
   * Validate a data request against a table layout.
   *
   * @param dataRequest A KijiDataRequest.
   * @param layout the KijiTableLayout of the table against which to validate the data request.
   */
  private void validateRequestAgainstLayout(KijiDataRequest dataRequest, KijiTableLayout layout) {
    // TODO(SCHEMA-263): This could be made more efficient if the layout and/or validator were
    // cached.
    KijiDataRequestValidator.validatorForLayout(layout).validate(dataRequest);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiTableReader instance %s in state %s.", this, oldState);
    mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
    mTable.release();
  }

  /** {@inheritDoc} */
  @Override
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      LOG.warn("Finalizing unclosed KijiTableReader {} in state {}.", this, state);
      close();
    }
    super.finalize();
  }
}
