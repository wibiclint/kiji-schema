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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.filter.Filters;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.filter.KijiColumnRangeFilter;
import org.kiji.schema.filter.StripValueColumnFilter;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.KijiPaginationFilter;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.impl.hbase.HBaseDataRequestAdapter;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;
import org.kiji.schema.util.Debug;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Pages through the many qualifiers of a map-type family.
 *
 * <p>
 *   The max-versions parameter on a map-type family applies on a per-qualifier basis.
 *   This does not limit the total number of versions returned for the entire map-type family.
 * </p>
 */
@ApiAudience.Private
public final class CassandraQualifierPager implements Iterator<String[]>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraQualifierPager.class);

  /** Entity ID of the row being paged through. */
  private final EntityId mEntityId;

  /** KijiTable to read from. */
  private final CassandraKijiTable mTable;

  /** Name of the map-type family being paged through. */
  private final KijiColumnName mFamily;

  /** Full data request. */
  private final KijiDataRequest mDataRequest;

  /** Column data request for the map-type family to page through. */
  private final KijiDataRequest.Column mColumnRequest;

  /** True only if there is another page of data to read through {@link #next()}. */
  private boolean mHasNext;

  /** Paged Iterator over all of the Rows coming back from Cassandra. */
  private Iterator<Row> mRowIterator;

  /**
   * Highest qualifier (according to the HBase bytes comparator) returned so far.
   * This is the low bound (exclusive) for qualifiers to retrieve next.
   */
  private String mMinQualifier = null;


  private final CassandraColumnNameTranslator mColumnNameTranslator;

  /**
   * Initializes a qualifier pager.
   *
   * @param entityId The entityId of the row.
   * @param dataRequest The requested data.
   * @param table The Kiji table that this row belongs to.
   * @param family Iterate through the qualifiers from this map-type family.
   * @throws org.kiji.schema.KijiColumnPagingNotEnabledException If paging is not enabled for the specified family.
   */
  public CassandraQualifierPager(
      EntityId entityId,
      KijiDataRequest dataRequest,
      CassandraKijiTable table,
      KijiColumnName family)
      throws KijiColumnPagingNotEnabledException {

    Preconditions.checkArgument(!family.isFullyQualified(),
        "Must use HBaseQualifierPager on a map-type family, but got '{}'.", family);
    mFamily = family;

    mDataRequest = dataRequest;
    mColumnRequest = mDataRequest.getColumn(family.getFamily(), null);
    if (!mColumnRequest.isPagingEnabled()) {
      throw new KijiColumnPagingNotEnabledException(
        String.format("Paging is not enabled for column [%s].", family));
    }

    mColumnNameTranslator = (CassandraColumnNameTranslator) table.getColumnNameTranslator();

    mEntityId = entityId;
    mTable = table;
    mHasNext = true;  // there might be no page to read, but we don't know until we issue an RPC

    initializeRowIterator();


    // Only retain the table if everything else ran fine:
    mTable.retain();
  }

  private void initializeRowIterator() {
    // Issue a paged SELECT statement to get all of the qualifiers for this map family from C*.
    // Get the Cassandra table name for this column family
    String cassandraTableName = KijiManagedCassandraTableName.getKijiTableName(
        mTable.getURI(),
        mTable.getName()).toString();

    // Get the translated name for the column family.
    String translatedLocalityGroup = null;
    String translatedFamily = null;
    try {
      translatedLocalityGroup = mColumnNameTranslator.toCassandraLocalityGroup(mFamily);
      translatedFamily = mColumnNameTranslator.toCassandraColumnFamily(mFamily);
    } catch (NoSuchColumnException nsce) {
      // TODO: Do something here!
      assert(false);
      return;
    }

    // TODO: prepare this statement only once.
    // Note that there is no way to get distinct qualifiers (without creating a new,
    // qualifier-only table of course.
    String queryString = String.format(
        "SELECT %s from %s WHERE %s=? AND %s=? AND %s=? AND %s=?",
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        cassandraTableName,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL
    );

    Session session = mTable.getAdmin().getSession();
    PreparedStatement preparedStatement = session.prepare(queryString);
    BoundStatement boundStatement = preparedStatement.bind(
        CassandraByteUtil.bytesToByteBuffer(mEntityId.getHBaseRowKey()),
        translatedLocalityGroup,
        translatedFamily
    );
    boundStatement.setFetchSize(mColumnRequest.getPageSize());
    mRowIterator = session.execute(boundStatement).iterator();
  }


  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return mHasNext;
  }

  /** {@inheritDoc} */
  @Override
  public String[] next() {
    return next(mColumnRequest.getPageSize());
  }

  /**
   * Fetches another page of qualifiers.
   *
   * @param pageSize Maximum number of qualifiers to retrieve in the page.
   * @return the next page of qualifiers.
   */
  public String[] next(int pageSize) {
    if (!mHasNext) {
      throw new NoSuchElementException();
    }
    Preconditions.checkArgument(pageSize > 0, "Page size must be >= 1, got %s", pageSize);

    HashSet<String> qualifiers = new HashSet<String>();

    while (qualifiers.size() < pageSize && mRowIterator.hasNext()) {
      qualifiers.add(mRowIterator.next().getString(CassandraKiji.CASSANDRA_QUALIFIER_COL));
    }
     return qualifiers.toArray(new String[0]);
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("KijiPager.remove() is not supported.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mTable.release();
  }
}
