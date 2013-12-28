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

import com.google.common.base.Preconditions;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Cassandra implementation of KijiPager for map-type families.
 *
 * <p>
 *   This pager lists the qualifiers in the map-type family and nothing else.
 *   In particular, the cells content is not retrieved.
 * </p>
 *
 * <p>
 *   This pager conforms to the KijiPager interface, in order to implement
 *   {@link org.kiji.schema.KijiRowData#getPager(String)}.
 *   More straightforward interfaces are available using {@link org.kiji.schema.impl.cassandra.CassandraQualifierPager} and
 *   {@link org.kiji.schema.impl.cassandra.CassandraQualifierIterator}.
 * </p>
 *
 * @see org.kiji.schema.impl.cassandra.CassandraQualifierPager
 * @see org.kiji.schema.impl.cassandra.CassandraQualifierIterator
 */
@ApiAudience.Private
public final class CassandraMapFamilyPager implements KijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraMapFamilyPager.class);

  private final CassandraQualifierPager mQualifierPager;
  private final String mFamily;
  private final CassandraKijiTable mTable;
  private final KijiDataRequest mDataRequest;
  private final EntityId mEntityId;

  CassandraMapFamilyPager(
      EntityId entityId,
      KijiDataRequest dataRequest,
      CassandraKijiTable table,
      KijiColumnName family)
      throws KijiColumnPagingNotEnabledException {

    Preconditions.checkArgument(!family.isFullyQualified(),
        "Must use HBaseQualifierPager on a map-type family, but got '{}'.", family);

    mQualifierPager = new CassandraQualifierPager(entityId, dataRequest, table, family);
    mFamily = family.toString();
    mTable = table;
    mDataRequest = dataRequest;
    mEntityId = entityId;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    return mQualifierPager.hasNext();
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData next() {
    String[] nextQualifiers = mQualifierPager.next();
    try {
      return createRowDataFromQualifiers(nextQualifiers);
    } catch (IOException e) {
      // TODO: Do something else here?
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData next(int pageSize) {
    String[] nextQualifiers = mQualifierPager.next(pageSize);
    try {
      return createRowDataFromQualifiers(nextQualifiers);
    } catch (IOException e) {
      // TODO: Do something else here?
      return null;
    }
  }

  private KijiRowData createRowDataFromQualifiers(String[] qualifiers) throws IOException {
    NavigableMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>> map =
        new TreeMap<String, NavigableMap<String, NavigableMap<Long, byte[]>>>();

    // Create an entry for the top-level family.
    map.put(
        mFamily,
        new TreeMap<String, NavigableMap<Long, byte[]>>()
    );

    // Create entries for each unique qualifier.
    for (String qualifier : qualifiers) {
      map.get(mFamily).put(qualifier, new TreeMap<Long, byte[]>());
    }

    return new CassandraKijiRowData(mTable, mDataRequest, mEntityId, map, null);
  }

  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("KijiPager.remove() is not supported.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mQualifierPager.close();
  }
}
