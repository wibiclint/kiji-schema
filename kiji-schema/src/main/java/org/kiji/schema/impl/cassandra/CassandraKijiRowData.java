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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.impl.BoundColumnReaderSpec;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.util.TimestampComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * An implementation of KijiRowData that wraps an Cassandra Result object.
 */
@ApiAudience.Private
public final class CassandraKijiRowData implements KijiRowData {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiRowData.class);

  /** The entity id for the row. */
  private final EntityId mEntityId;

  /** The request used to retrieve this Kiji row data. */
  private final KijiDataRequest mDataRequest;

  /** The Cassandra KijiTable we are reading from. */
  private final CassandraKijiTable mTable;

  /** The layout for the table this row data came from. */
  private final KijiTableLayout mTableLayout;

  /** The Cassandra result providing the data of this object. */
  private Collection<Row> mRows;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mDecoderProvider;

  /** A map from kiji family to kiji qualifier to timestamp to raw encoded cell values. */
  private NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> mFilteredMap;

  /**
   * Creates a provider for cell decoders.
   *
   * <p> The provider creates decoders for specific Avro records. </p>
   *
   * @param table Cassandra KijiTable to create a CellDecoderProvider for.
   * @return a new CellDecoderProvider for the specified Cassandra KijiTable.
   * @throws java.io.IOException on I/O error.
   */
  private static CellDecoderProvider createCellProvider(CassandraKijiTable table) throws IOException {
    final LayoutCapsule capsule = table.getLayoutCapsule();
    return new CellDecoderProvider(
        capsule.getLayout(),
        Maps.<KijiColumnName, BoundColumnReaderSpec>newHashMap(),
        Sets.<BoundColumnReaderSpec>newHashSet(),
        KijiTableReaderBuilder.DEFAULT_CACHE_MISS);
  }

  /**
   * Initializes a row data from an Cassandra Result.
   *
   * <p>
   *   The Cassandra Result may contain more cells than are requested by the user.
   *   KijiDataRequest is more expressive than Cassandra Get/Scan requests.
   *   Currently, {@link #getMap()} attempts to complete the filtering to meet the data request
   *   requirements expressed by the user, but this can be inaccurate.
   * </p>
   *
   * @param table Kiji table containing this row.
   * @param dataRequest Data requested for this row.
   * @param entityId This row entity ID.
   * @param rows Cassandra result containing the requested cells (and potentially more).
   * @param decoderProvider Provider for cell decoders.
   *     Null means the row creates its own provider for cell decoders (not recommended).
   * @throws java.io.IOException on I/O error.
   */
  public CassandraKijiRowData(
      CassandraKijiTable table,
      KijiDataRequest dataRequest,
      EntityId entityId,
      Collection<Row> rows,
      CellDecoderProvider decoderProvider)
      throws IOException {
    mTable = table;
    mTableLayout = table.getLayout();
    mDataRequest = dataRequest;
    mEntityId = entityId;
    mRows = rows;
    mDecoderProvider = (decoderProvider != null) ? decoderProvider : createCellProvider(table);
  }

  /**
   * Get the decoder for the given column from the {@link org.kiji.schema.layout.impl.CellDecoderProvider}.
   *
   * @param column column for which to get a cell decoder.
   * @param <T> the type of the value encoded in the cell.
   * @return a cell decoder which can read the given column.
   * @throws java.io.IOException in case of an error getting the cell decoder.
   */
  private <T> KijiCellDecoder<T> getDecoder(KijiColumnName column) throws IOException {
    final KijiDataRequest.Column requestColumn = mDataRequest.getRequestForColumn(column);
    if (null != requestColumn) {
      final ColumnReaderSpec spec = requestColumn.getReaderSpec();
      if (null != spec) {
        // If there is a spec override in the data request, use it to get the decoder.
        return mDecoderProvider.getDecoder(BoundColumnReaderSpec.create(spec, column));
      }
    }
    // If the column is not in the request, or there is no spec override, get the decoder for the
    // column by name.
    return mDecoderProvider.getDecoder(column.getFamily(), column.getQualifier());
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId() {
    return mEntityId;
  }

  /**
   * Gets the data request used to retrieve this row data.
   *
   * @return the data request used to retrieve this row data.
   */
  public KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /**
   * Gets the layout of the table this row data belongs to.
   *
   * @return The table layout.
   */
  public KijiTableLayout getTableLayout() {
    return mTableLayout;
  }

  /**
   * Gets a map from kiji family to qualifier to timestamp to raw kiji-encoded bytes of a cell.
   *
   * @return The map.
   */
  public synchronized NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>>
      getMap() {
    if (null != mFilteredMap) {
      return mFilteredMap;
    }

    LOG.info("Filtering the Cassandra results into a map of kiji cells...");

    mFilteredMap = new TreeMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>>();

    // Go through every column in the result set and add the data to the filtered map.
    for (Row row : mRows) {

      // Get the Cassandra key (entity Id), qualifier, timestamp, and value.
      ByteBuffer eidByteBuffer = row.getBytes(CassandraKiji.CASSANDRA_KEY_COL);
      String family = row.getString(CassandraKiji.CASSANDRA_FAMILY_COL);
      String qualifier = row.getString(CassandraKiji.CASSANDRA_QUALIFIER_COL);
      Long timestamp = row.getLong(CassandraKiji.CASSANDRA_VERSION_COL);
      ByteBuffer value = row.getBytes(CassandraKiji.CASSANDRA_VALUE_COL);

      //LOG.info("Got back data from table for qualifier " + qualifier + " and timestamp " + timestamp);

      // Insert this data into the map.

      // Get a reference to the map for the family.
      NavigableMap<String, NavigableMap<Long, byte[]>> familyMap;
      if (mFilteredMap.containsKey(family)) {
        familyMap = mFilteredMap.get(family);
      } else {
        familyMap = new TreeMap<String, NavigableMap<Long, byte[]>>();
        mFilteredMap.put(family, familyMap);
      }

      // Get a reference to the map for the qualifier.
      NavigableMap<Long, byte[]> qualifierMap;
      if (familyMap.containsKey(qualifier)) {
        qualifierMap = familyMap.get(qualifier);
      } else {
        qualifierMap = new TreeMap<Long, byte[]>();
        familyMap.put(qualifier, qualifierMap);
      }

      // Should not already have an entry for this timestamp!
      assert(!qualifierMap.containsKey(timestamp));

      // Finally insert the data into the map!
      qualifierMap.put(timestamp, CassandraByteUtil.byteBuffertoBytes(value));
    }
    return mFilteredMap;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsColumn(String family, String qualifier) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return false;
    }
   final  NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
    if (null == versionMap) {
      return false;
    }
    return !versionMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsColumn(String family) {

    final NavigableMap<String, NavigableMap<Long, byte[]>> columnMap = getMap().get(family);
    if (null == columnMap) {
      return false;
    }
    for (Map.Entry<String, NavigableMap<String, NavigableMap<Long, byte[]>>> columnMapEntry
      : getMap().entrySet()) {
      LOG.debug("The result return contains family [{}]", columnMapEntry.getKey());
    }
    return !columnMap.isEmpty();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean containsCell(String family, String qualifier, long timestamp) {
    return containsColumn(family, qualifier)
        && getTimestamps(family, qualifier).contains(timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<String> getQualifiers(String family) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> qmap = getRawQualifierMap(family);
    if (null == qmap) {
      return Sets.newTreeSet();
    }
    return qmap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized NavigableSet<Long> getTimestamps(String family, String qualifier) {
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return Sets.newTreeSet(TimestampComparator.INSTANCE);
    }
    return tmap.navigableKeySet();
  }

  /** {@inheritDoc} */
  @Override
  public Schema getReaderSchema(String family, String qualifier) throws IOException {
    return mTableLayout.getCellSpec(new KijiColumnName(family, qualifier)).getAvroSchema();
  }

  /**
   * Reports the encoded map of qualifiers of a given family.
   *
   * @param family Family to look up.
   * @return the encoded map of qualifiers in the specified family, or null.
   */
  private NavigableMap<String, NavigableMap<Long, byte[]>> getRawQualifierMap(String family) {
    return getMap().get(family);
  }

  /**
   * Reports the specified raw encoded time-series of a given column.
   *
   * @param family Family to look up.
   * @param qualifier Qualifier to look up.
   * @return the encoded time-series in the specified family:qualifier column, or null.
   */
  private NavigableMap<Long, byte[]> getRawTimestampMap(String family, String qualifier) {
    final NavigableMap<String, NavigableMap<Long, byte[]>> qmap = getRawQualifierMap(family);
    if (null == qmap) {
      return null;
    }
    return qmap.get(qualifier);
  }

  /**
   * Reports the encoded content of a given cell.
   *
   * @param family Family to look up.
   * @param qualifier Qualifier to look up.
   * @param timestamp Timestamp to look up.
   * @return the encoded cell content, or null.
   */
  private byte[] getRawCell(String family, String qualifier, long timestamp) {
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    return tmap.get(timestamp);
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getValue(String family, String qualifier, long timestamp) throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getCell(String family, String qualifier, long timestamp)
      throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));
    final byte[] bytes = getRawCell(family, qualifier, timestamp);
    return new KijiCell<T>(family, qualifier, timestamp, decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> T getMostRecentValue(String family, String qualifier) throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    final byte[] bytes = tmap.values().iterator().next();
    return decoder.decodeValue(bytes);
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, T> getMostRecentValues(String family) throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentValues(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getMostRecentValues(String family, String qualifier) method.",
        family);
    final NavigableMap<String, T> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final T value = getMostRecentValue(family, qualifier);
      result.put(qualifier, value);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, T> getValues(String family, String qualifier)
      throws IOException {
    final NavigableMap<Long, T> result = Maps.newTreeMap(TimestampComparator.INSTANCE);
    for (Map.Entry<Long, KijiCell<T>> entry : this.<T>getCells(family, qualifier).entrySet()) {
      result.put(entry.getKey(), entry.getValue().getData());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, T>> getValues(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getValues(String family) is only enabled on map "
        + "type column families. The column family [%s], is a group type column family. Please use "
        + "the getValues(String family, String qualifier) method.",
        family);
    final NavigableMap<String, NavigableMap<Long, T>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final NavigableMap<Long, T> timeseries = getValues(family, qualifier);
      result.put(qualifier, timeseries);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> KijiCell<T> getMostRecentCell(String family, String qualifier) throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (null == tmap) {
      return null;
    }
    final byte[] bytes = tmap.values().iterator().next();
    final long timestamp = tmap.firstKey();
    return new KijiCell<T>(family, qualifier, timestamp, decoder.decodeCell(bytes));
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, KijiCell<T>> getMostRecentCells(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getMostRecentCells(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getMostRecentCells(String family, String qualifier) method.",
        family);
    final NavigableMap<String, KijiCell<T>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final KijiCell<T> cell = getMostRecentCell(family, qualifier);
      result.put(qualifier, cell);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<Long, KijiCell<T>> getCells(String family, String qualifier)
      throws IOException {
    final KijiCellDecoder<T> decoder = getDecoder(new KijiColumnName(family, qualifier));

    final NavigableMap<Long, KijiCell<T>> result = Maps.newTreeMap(TimestampComparator.INSTANCE);
    final NavigableMap<Long, byte[]> tmap = getRawTimestampMap(family, qualifier);
    if (tmap != null) {
      for (Map.Entry<Long, byte[]> entry : tmap.entrySet()) {
        final Long timestamp = entry.getKey();
        final byte[] bytes = entry.getValue();
        final KijiCell<T> cell =
            new KijiCell<T>(family, qualifier, timestamp, decoder.decodeCell(bytes));
        result.put(timestamp, cell);
      }
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> NavigableMap<String, NavigableMap<Long, KijiCell<T>>> getCells(String family)
      throws IOException {
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getCells(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the getCells(String family, String qualifier) method.",
        family);
    final NavigableMap<String, NavigableMap<Long, KijiCell<T>>> result = Maps.newTreeMap();
    for (String qualifier : getQualifiers(family)) {
      final NavigableMap<Long, KijiCell<T>> cells = getCells(family, qualifier);
      result.put(qualifier, cells);
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family, String qualifier)
      throws IOException {
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    return new KijiCellIterator<T>(column, this, mEntityId);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterator<KijiCell<T>> iterator(String family)
      throws IOException {
    final KijiColumnName column = new KijiColumnName(family, null);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "iterator(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the iterator(String family, String qualifier) method.",
        family);
    return new KijiCellIterator<T>(column, this, mEntityId);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family, String qualifier) {
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    return new CellIterable<T>(column, this, mEntityId);
  }

  /** {@inheritDoc} */
  @Override
  public <T> Iterable<KijiCell<T>> asIterable(String family) {
    final KijiColumnName column = new KijiColumnName(family, null);
    Preconditions.checkArgument(
        mDataRequest.getRequestForColumn(column) != null,
        "Column %s has no data request.", column);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "asIterable(String family) is only enabled"
        + " on map type column families. The column family [%s], is a group type column family."
        + " Please use the asIterable(String family, String qualifier) method.",
        family);
    return new CellIterable<T>(column, this, mEntityId);
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family, String qualifier)
      throws KijiColumnPagingNotEnabledException {
    /*
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);
    return new CassandraVersionPager(
        mEntityId, mDataRequest, mTable,  kijiColumnName, mDecoderProvider);
    */
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public KijiPager getPager(String family) throws KijiColumnPagingNotEnabledException {
    /*
    final KijiColumnName kijiFamily = new KijiColumnName(family, null);
    Preconditions.checkState(mTableLayout.getFamilyMap().get(family).isMapType(),
        "getPager(String family) is only enabled on map type column families. "
        + "The column family '%s' is a group type column family. "
        + "Please use the getPager(String family, String qualifier) method.",
        family);
    return new CassandraMapFamilyPager(mEntityId, mDataRequest, mTable, kijiFamily);
    */
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraKijiRowData.class)
        .add("table", mTable.getURI())
        .add("entityId", getEntityId())
        .add("dataRequest", mDataRequest)
        //.add("resultSize", mResult.size())
        //.add("result", mResult)
        .add("map", getMap())
        .toString();
  }

  /**
   * An iterator for cells in group type column or map type column family.
   *
   * @param <T> The type parameter for the KijiCells being iterated over.
   */
  private static final class KijiCellIterator<T> implements Iterator<KijiCell<T>> {
    /** A KeyValue comparator. */
    //private static final KVComparator KV_COMPARATOR = new KVComparator();
    /** The cell decoder for this column. */
    //private final KijiCellDecoder<T> mDecoder;
    /** The column name translator for the given table. */
    //private final ColumnNameTranslator mColumnNameTranslator;
    /** The maximum number of versions requested. */
    //private final int mMaxVersions;
    /** An array of KeyValues returned by Cassandra. */
    //private final KeyValue[] mKVs;
    /** The column or map type family being iterated over. */
    //private final KijiColumnName mColumn;
    /** The number of versions returned by the iterator so far. */
    //private int mNumVersions;
    /** The current index in the underlying KV array. */
    //private int mCurrentIdx;
    /** The next cell to return. */
    //private KijiCell<T> mNextCell;

    /**
     * An iterator of KijiCells, for a particular column.
     *
     * @param columnName The Kiji column that is being iterated over.
     * @param rowdata The CassandraKijiRowData instance containing the desired data.
     * @param eId of the rowdata we are iterating over.
     * @throws java.io.IOException on I/O error
     */
    protected KijiCellIterator(KijiColumnName columnName, CassandraKijiRowData rowdata, EntityId eId)
        throws IOException {
      /*
      mColumn = columnName;
      // Initialize column name translator.
      mColumnNameTranslator = new ColumnNameTranslator(rowdata.mTableLayout);
      // Get cell decoder.
      mDecoder = rowdata.getDecoder(mColumn);
      // Get info about the data request for this column.
      KijiDataRequest.Column columnRequest = rowdata.mDataRequest.getRequestForColumn(mColumn);
      mMaxVersions = columnRequest.getMaxVersions();
      mKVs = rowdata.mResult.raw();
      mNumVersions = 0;
      // Find the first index for this column.
      final CassandraColumnName colName = mColumnNameTranslator.toHBaseColumnName(mColumn);
      mCurrentIdx = findInsertionPoint(mKVs, new KeyValue(eId.getCassandraRowKey(), colName.getFamily(),
          colName.getQualifier()));
      mNextCell = getNextCell();
      */
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      //return (null != mNextCell);
      return false;
    }

    /** {@inheritDoc} */
    @Override
    public KijiCell<T> next() {
      /*
      KijiCell<T> cellToReturn = mNextCell;
      if (null == mNextCell) {
        throw new NoSuchElementException();
      } else {
        mNumVersions += 1;
      }
      mCurrentIdx = getNextIndex(mCurrentIdx);
      mNextCell = getNextCell();
      return cellToReturn;
      */
      return null;
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Removing a cell is not a supported operation.");
    }

    /**
     * Constructs the next cell that will be returned by the iterator.
     *
     * @return The next cell in the column we are iterating over, potentially null.
     */
    private KijiCell<T> getNextCell() {
      /*
      KijiCell<T> nextCell = null;
      try {
        if (mCurrentIdx < mKVs.length) { // If our index is out of bounds, nextCell is null.
          final KeyValue kv = mKVs[mCurrentIdx];
          // Filter KeyValues by Kiji column family.
          final KijiColumnName colName = mColumnNameTranslator.toKijiColumnName(
              new CassandraColumnName(kv.getFamily(), kv.getQualifier()));
          nextCell = new KijiCell<T>(mColumn.getFamily(), colName.getQualifier(),
              kv.getTimestamp(), mDecoder.decodeCell(kv.getValue()));
        }
      } catch (IOException ex) {
        throw new KijiIOException(ex);
      }
      return nextCell;
      */
      return null;
    }

    /**
     * Finds the index of the next KeyValue in the underlying KV array that is from the column we
     * are iterating over.
     *
     * @param lastIndex The index of the KV used to construct the last returned cell.
     * @return The index of the next KeyValue from the column we are iterating over.If there are no
     * more cells to iterate over, the returned value will be mKVs.length.
     */
    private int getNextIndex(int lastIndex) {
      /*
      int nextIndex = lastIndex;
      if (mColumn.isFullyQualified()) {
        if (mNumVersions < mMaxVersions) {
          nextIndex = lastIndex + 1; // The next element should be the next in the array.
        } else {
          nextIndex = mKVs.length; //There is nothing else to return.
        }
      } else {
        if (mNumVersions <= mMaxVersions) {
          nextIndex = lastIndex + 1; // The next element should be the next in the array.
        } else {
          mNumVersions = 0; // Reset current number of versions.
          nextIndex = findInsertionPoint(mKVs, makePivotKeyValue(mKVs[lastIndex]));
        }
      }
      if (nextIndex < mKVs.length) {
        final KeyValue kv = mKVs[nextIndex];
        // Filter KeyValues by Kiji column family.
        try {
          final KijiColumnName colName = mColumnNameTranslator.toKijiColumnName(
            new CassandraColumnName(kv.getFamily(), kv.getQualifier()));
          if (!colName.getQualifier().equals(mNextCell.getQualifier())) {
            if (mColumn.isFullyQualified()) {
              return mKVs.length;
            } else {
              nextIndex = findInsertionPoint(mKVs, makePivotKeyValue(mKVs[lastIndex]));
              mNumVersions = 0;
            }
          }
          if (!colName.getFamily().equals(mColumn.getFamily())) {
            return mKVs.length; // From the wrong column family.
          }
        } catch (NoSuchColumnException ex) {
          throw new KijiIOException(ex);
        }
      }
      return nextIndex;
      */
      return -1;
    }

  }
  /**
   * An iterable of cells in a column.
   *
   * @param <T> The type parameter for the KijiCells being iterated over.
   */
  private static final class CellIterable<T> implements Iterable<KijiCell<T>> {
    /** The column family. */
    //private final KijiColumnName mColumnName;
    /** The rowdata we are iterating over. */
    //private final CassandraKijiRowData mRowData;
    /** The entity id for the row. */
    //private final EntityId mEntityId;
    /**
     * An iterable of KijiCells, for a particular column.
     *
     * @param colName The Kiji column family that is being iterated over.
     * @param rowdata The CassandraKijiRowData instance containing the desired data.
     * @param eId of the rowdata we are iterating over.
     */
    protected CellIterable(KijiColumnName colName, CassandraKijiRowData rowdata, EntityId eId) {
      /*
      mColumnName = colName;
      mRowData = rowdata;
      mEntityId = eId;
      */
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<KijiCell<T>> iterator() {
      /*
      try {
        return new KijiCellIterator<T>(mColumnName , mRowData, mEntityId);
      } catch (IOException ex) {
        throw new KijiIOException(ex);
      }
      */
      return null;
    }
  }

}
