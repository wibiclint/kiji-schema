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
import com.datastax.driver.core.Session;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps a KijiDataRequest to expose methods that generate meaningful objects in Cassandra land.
 */
@ApiAudience.Private
public class CassandraDataRequestAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraDataRequestAdapter.class);

  /** The wrapped KijiDataRequest. */
  private final KijiDataRequest mKijiDataRequest;

  /** The translator for generating Cassandra column names. */
  private final ColumnNameTranslator mColumnNameTranslator;

  /**

  /**
   * Creates a new CassandraDataRequestAdapter for a given data request using a given
   * ColumnNameTranslator.
   *
   * @param kijiDataRequest The data request to adapt for Cassandra.
   * @param translator The name translator for getting Cassandra column names.
   */
  public CassandraDataRequestAdapter(
      KijiDataRequest kijiDataRequest,
      ColumnNameTranslator translator) {
    mKijiDataRequest = kijiDataRequest;
    mColumnNameTranslator = translator;
  }

  /**
   * Executes the Cassandra scan request and returns a list of `ResultSet` objects.
   */
  public List<ResultSet> doScan(
      CassandraKijiTable table,
      KijiTableLayout kijiTableLayout,
      KijiTableReader.KijiScannerOptions kijiScannerOptions
    ) throws IOException {
    // TODO: Do something with the scan options.
    return queryCassandraTables(table, null, kijiTableLayout);
  }

  /**
   *
   * @param table
   * @param entityId EntityID for the given get.
   * @param kijiTableLayout
   * @return
   * @throws IOException
   */
  public List<ResultSet> doGet(
      CassandraKijiTable table,
      EntityId entityId,
      KijiTableLayout kijiTableLayout
  ) throws IOException {
    return queryCassandraTables(table, entityId, kijiTableLayout);
  }

  /**
   * Query a Cassandra table for a get or for a scan.
   *
   * @param table The Cassandra Kiji table to scan.
   * @param entityId Make null if this is a scan over all entity IDs (not currently supporting entity ID ranges).
   * @param kijiTableLayout The layout for the Cassandra Kiji table to scan.
   * @return A list of results for the Cassandra query.
   * @throws IOException
   */
  private List<ResultSet> queryCassandraTables(
      CassandraKijiTable table,
      EntityId entityId,
      KijiTableLayout kijiTableLayout
  ) throws IOException {
    boolean bIsScan = (null == entityId);
    Session session = table.getAdmin().getSession();

    // Keep track of all of the results coming back from Cassandra
    ArrayList<ResultSet> results = new ArrayList<ResultSet>();

    ByteBuffer entityIdByteBuffer;
    if (bIsScan) {
      entityIdByteBuffer = null;
    } else {
      entityIdByteBuffer = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());
    }

    // Ignore everything for now except for column families and qualifiers.
    // For now, to keep things simple, we have a separate request for each column (even if there
    // are multiple columns of interest in the same column family / C* table).
    for (KijiDataRequest.Column column : mKijiDataRequest.getColumns()) {

      // Get the Cassandra table name for this column family
      String cassandraTableName = KijiManagedCassandraTableName.getKijiTableName(
          table.getURI(),
          table.getName()).toString();

      // TODO: Optimize these queries such that we need only one RPC per column family.
      // (Right now a data request that asks for "info:foo" and "info:bar" would trigger two
      // separate session.execute(statement) commands.

      // Get the translated Kiji family and qualifier.
      HBaseColumnName hBaseColumnName = mColumnNameTranslator.toHBaseColumnName(
          new KijiColumnName(column.getName())
      );
      String family = hBaseColumnName.getFamilyAsString();
      String qualifier = hBaseColumnName.getQualifierAsString();

      if (bIsScan) {
        // Select this column in the C* family for this qualifier.
        // Eventually, this column will to have escaped quotes around it (to handle upper and lower case)
        String queryString = String.format(
            "SELECT token(%s), %s, %s, %s, %s, %s FROM %s WHERE %s=? AND %s=? ALLOW FILTERING",
            CassandraKiji.CASSANDRA_KEY_COL,
            CassandraKiji.CASSANDRA_KEY_COL,
            CassandraKiji.CASSANDRA_FAMILY_COL,
            CassandraKiji.CASSANDRA_QUALIFIER_COL,
            CassandraKiji.CASSANDRA_VERSION_COL,
            CassandraKiji.CASSANDRA_VALUE_COL,
            cassandraTableName,
            CassandraKiji.CASSANDRA_FAMILY_COL,
            CassandraKiji.CASSANDRA_QUALIFIER_COL
        );
        LOG.info("Preparing query string " + queryString);


        PreparedStatement preparedStatement = session.prepare(queryString);
        ResultSet res = session.execute(preparedStatement.bind(family, qualifier));
        results.add(res);
      } else {
        assert(entityId != null);

        // Select this column in the C* family for this qualifier.
        // Eventually, this column will to have escaped quotes around it (to handle upper and lower case)
        String queryString = String.format(
            "SELECT * FROM %s WHERE %s=? AND %s=? AND %s=?",
            cassandraTableName,
            CassandraKiji.CASSANDRA_KEY_COL,
            CassandraKiji.CASSANDRA_FAMILY_COL,
            CassandraKiji.CASSANDRA_QUALIFIER_COL
        );
        LOG.info("Preparing query string " + queryString);

        PreparedStatement preparedStatement = session.prepare(queryString);
        ResultSet res = session.execute(preparedStatement.bind(entityIdByteBuffer, family, qualifier));
        results.add(res);
      }
    }
    return results;
  }
}
