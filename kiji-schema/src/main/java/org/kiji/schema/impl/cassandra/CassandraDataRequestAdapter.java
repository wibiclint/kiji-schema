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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.filter.KijiColumnFilter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Given a KijiDataRequest, create Cassandra queries.
 */
@ApiAudience.Private
public class CassandraDataRequestAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraDataRequestAdapter.class);

  /** The wrapped KijiDataRequest. */
  private final KijiDataRequest mKijiDataRequest;

  /** The translator for generating Cassandra column names. */
  private final ColumnNameTranslator mColumnNameTranslator;

  /**
   * Creates a new CassandraDataRequestAdapter for a given data request using a given
   * ColumnNameTranslator.
   *
   * @param kijiDataRequest the data request to adapt for Cassandra.
   * @param translator the name translator for getting Cassandra column names.
   */
  public CassandraDataRequestAdapter(KijiDataRequest kijiDataRequest, ColumnNameTranslator translator) {
    mKijiDataRequest = kijiDataRequest;
    mColumnNameTranslator = translator;
  }

  /**
   * Constructs a Cassandra query that describes the data requested in the KijiDataRequest for
   * a particular entity/row.
   *
   * @param entityId The row to build an Cassandra Get request for.
   * @param tableLayout The layout of the Kiji table to read from.  This is required for
   *     determining the mapping between Kiji columns and Cassandra columns.
   * @return An Cassandra query descriptor.
   * @throws java.io.IOException If there is an error.
   */
  public String toGet(EntityId entityId, KijiTableLayout tableLayout)
      throws IOException {

    // TODO: Implement Cassandra Kiji gets.
    return null;
  }
}
