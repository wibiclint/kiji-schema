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
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.*;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
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
  private final CassandraColumnNameTranslator mColumnNameTranslator;

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
      CassandraColumnNameTranslator translator) {
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
    return queryCassandraTables(table, null, kijiTableLayout, false);
  }

  /**
   * Perform a Cassandra CQL "SELECT" statement (like an HBase Get).  Will ignore any columns with
   * paging enabled (use `doPagedGet` for requests with paging).
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
    return queryCassandraTables(table, entityId, kijiTableLayout, false);
  }

  public List<ResultSet> doPagedGet(
      CassandraKijiTable table,
      EntityId entityId,
      KijiTableLayout kijiTableLayout
  ) throws IOException {
    return queryCassandraTables(table, entityId, kijiTableLayout, true);
  }

  /**
   * Query a Cassandra table for a get or for a scan.
   *
   * @param table The Cassandra Kiji table to scan.
   * @param entityId Make null if this is a scan over all entity IDs (not currently supporting entity ID ranges).
   * @param kijiTableLayout The layout for the Cassandra Kiji table to scan.
   * @param pagingEnabled If true, all columns in the data request should be paged.  If false, skip
   *                      any paged columns in the data request.
   * @return A list of results for the Cassandra query.
   * @throws IOException
   */
  private List<ResultSet> queryCassandraTables(
      CassandraKijiTable table,
      EntityId entityId,
      KijiTableLayout kijiTableLayout,
      boolean pagingEnabled
  ) throws IOException {
    // TODO: Or figure out how to combine multiple SELECT statements into a single RPC (IntraVert?).
    // TODO: Or just combine everything into a single SELECT statement!
    LOG.info("---------- Translating KijiDataRequest into Cassandra SELECT statements. ----------");
    boolean bIsScan = (null == entityId);

    // Cannot do a scan with paging.
    Preconditions.checkArgument(!(pagingEnabled && bIsScan));

    // Get the Cassandra table name for non-counter values.
    String nonCounterTableName = KijiManagedCassandraTableName.getKijiTableName(
        table.getURI(),
        table.getName()).toString();

    // Get the counter table name.
    String counterTableName = KijiManagedCassandraTableName.getKijiCounterTableName(
        table.getURI(),
        table.getName()).toString();

    // A single Kiji data request can result in many Cassandra queries, so we use asynchronous IO
    // and keep a list of all of the futures that will contain results from Cassandra.
    HashSet<ResultSetFuture> futures = new HashSet<ResultSetFuture>();

    // If this is not a scan, format the entity ID for Cassandra.
    ByteBuffer entityIdByteBuffer;
    if (bIsScan) {
      entityIdByteBuffer = null;
    } else {
      entityIdByteBuffer = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());
    }

    // Timestamp limits for queries.
    long maxTimestamp = mKijiDataRequest.getMaxTimestamp();
    long minTimestamp = mKijiDataRequest.getMinTimestamp();

    // If this is a scan, you need to make sure that you execute at least one SELECT statement,
    // just to get *something* back for every entity ID.  If you do not do so, then a user could
    // create a scanner with a data request that has a single, paged column, and you would never
    // execute a SELECT query below (because we wait to execute paged SELECT queries) and so you
    // would never get an iterator back with any row keys at all!  Eek!
    int numScanQueries = 0;

    // Use the C* admin to send queries to the C* cluster.
    CassandraAdmin admin = table.getAdmin();

    // For now, to keep things simple, we have a separate request for each column, even if there
    // are multiple columns of interest in the same column family that we could potentially put
    // together into a single query.
    for (KijiDataRequest.Column column : mKijiDataRequest.getColumns()) {
      LOG.info("Processing data request for data request column " + column);

      if (!pagingEnabled && column.isPagingEnabled()) {
        // The user will have to use an explicit KijiPager to get this data.
        LOG.info("...this column is paged, but this is not a KijiPager request, skipping...");
        continue;
      }

      // Requests with paging enabled should come from only explicit KijiPagers, which should
      // create custom data requests for only paged columns.
      assert (!(pagingEnabled && !column.isPagingEnabled()));

      // Translate the Kiji column name.
      KijiColumnName kijiColumnName = new KijiColumnName(column.getName());
      LOG.info("Kiji column name for the requested column is " + kijiColumnName);
      String localityGroup = mColumnNameTranslator.toCassandraLocalityGroup(kijiColumnName);
      String family = mColumnNameTranslator.toCassandraColumnFamily(kijiColumnName);
      String qualifier = mColumnNameTranslator.toCassandraColumnQualifier(kijiColumnName);

      if (null == qualifier) {
        LOG.info("Column request is for an entire family.");
      } else {
        LOG.info("Column request is for a full-qualified, individual column.");
      }

      // TODO: Optimize these queries such that we need only one RPC per column family.
      // (Right now a data request that asks for "info:foo" and "info:bar" would trigger two
      // separate session.execute(statement) commands.

      // TODO: If unqualified group-type family, maybe read counter and non-counter values together!
      // For qualified columns and for map-type families, we can determine whether to retrieve
      // non-counter or counter values.
      boolean readCounterValues = maybeContainsCounterValues(table, kijiColumnName);
      boolean readNonCounterValues = maybeContainsNonCounterValues(table, kijiColumnName);

      if (readCounterValues) {
        LOG.info("This column may contain a counter.");
      }

      List<String> tableNames = new ArrayList();

      if (readNonCounterValues) {
        tableNames.add(nonCounterTableName);
      }

      if (readCounterValues) {
        tableNames.add(counterTableName);
      }

      for (String cassandraTableName : tableNames) {
        if (bIsScan) {
          numScanQueries += queryCassandraSingleColumnAndUpdateFuturesScan(
              admin,
              cassandraTableName,
              localityGroup,
              family,
              qualifier,
              minTimestamp,
              maxTimestamp,
              column,
              futures
          );
        } else {
          queryCassandraSingleColumnAndUpdateFuturesGet(
              entityIdByteBuffer,
              admin,
              cassandraTableName,
              localityGroup,
              family,
              qualifier,
              minTimestamp,
              maxTimestamp,
              column,
              pagingEnabled,
              futures
          );

        }
      }
    }

    if (bIsScan && (0 == numScanQueries)) {
      // Need to add a dummy scan here to make sure that we get back some data for every row.
      String queryString = String.format(
          "SELECT token(%s), %s FROM %s",
          CassandraKiji.CASSANDRA_KEY_COL,
          CassandraKiji.CASSANDRA_KEY_COL,
          nonCounterTableName
      );
      ResultSetFuture res = admin.executeAsync(queryString);
      futures.add(res);
    }

    // Wait until all of the futures are done.
    List<ResultSet> results = new ArrayList<ResultSet>();

    for (ResultSetFuture resultSetFuture: futures) {
      results.add(resultSetFuture.getUninterruptibly());
    }
    return results;
  }

  /**
   *  Check whether this column could specify non-counter values.  Return false iff this column
   *  name refers to a fully-qualified column of type COUNTER or a map-type family of type COUNTER.
   *
   */
  private boolean maybeContainsNonCounterValues(
      CassandraKijiTable table,
      KijiColumnName kijiColumnName
  ) throws IOException {
    boolean isNonCounter = true;
    try {
      // Pick a table name depending on whether this column is a counter or not.
      if (table
          .getLayoutCapsule()
          .getLayout()
          .getCellSpec(kijiColumnName)
          .isCounter()) {
        isNonCounter = false;
      }
    } catch (IllegalArgumentException e) {
      // There *could* be non-counter values here.
    }
    return isNonCounter;
  }

  /**
   *  Check whether this column could specify non-counter values.  Return false iff this column
   *  name refers to a fully-qualified column of type COUNTER or a map-type family of type COUNTER.
   *
   */
  private boolean maybeContainsCounterValues(
      CassandraKijiTable table,
      KijiColumnName kijiColumnName
  ) throws IOException {
    boolean isCounter = false;
    try {
      // Pick a table name depending on whether this column is a counter or not.
      isCounter = table
          .getLayoutCapsule()
          .getLayout()
          .getCellSpec(kijiColumnName)
          .isCounter();
    } catch (IllegalArgumentException e) {
      // There *could* be counters here.
      isCounter = true;
    }
    return isCounter;
  }

  private void queryCassandraSingleColumnAndUpdateFuturesGet(
      ByteBuffer entityIdByteBuffer,
      CassandraAdmin admin,
      String cassandraTableName,
      String translatedLocalityGroup,
      String translatedFamily,
      String translatedQualifier,
      Long minTimestamp,
      Long maxTimestamp,
      KijiDataRequest.Column column,
      boolean pagingEnabled,
      HashSet<ResultSetFuture> resultSetFutures) {

    Statement boundStatement;
    if (translatedQualifier != null) {
      // Fully-qualified get.
      // TODO: Depending on filters, we may have to drop the LIMIT here.
      // Let's say that a client makes a data request with a filter with a qualifier regex and
      // specifies maxVersions.  We cannot put the qualifier regex into
      String queryString = String.format(
          "SELECT * FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=? AND %s >= ? and %s < ? LIMIT ?",
          cassandraTableName,
          CassandraKiji.CASSANDRA_KEY_COL,
          CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
          CassandraKiji.CASSANDRA_FAMILY_COL,
          CassandraKiji.CASSANDRA_QUALIFIER_COL,
          CassandraKiji.CASSANDRA_VERSION_COL,
          CassandraKiji.CASSANDRA_VERSION_COL
      );

      LOG.info("Preparing query string for single-row get of fully-qualified column: " + queryString);
      LOG.info(String.format("\tUsing limit %d", column.getMaxVersions()));

      PreparedStatement preparedStatement = admin.getPreparedStatement(queryString);
      boundStatement = preparedStatement.bind(
          entityIdByteBuffer,
          translatedLocalityGroup,
          translatedFamily,
          translatedQualifier,
          minTimestamp,
          maxTimestamp,
          column.getMaxVersions());
    } else {
      String queryString = String.format(
          "SELECT * FROM %s WHERE %s=? AND %s=? AND %s=?",
          cassandraTableName,
          CassandraKiji.CASSANDRA_KEY_COL,
          CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
          CassandraKiji.CASSANDRA_FAMILY_COL
      );
      LOG.info("Preparing query string " + queryString);

      PreparedStatement preparedStatement = admin.getPreparedStatement(queryString);
      boundStatement = preparedStatement.bind(
          entityIdByteBuffer,
          translatedLocalityGroup,
          translatedFamily
      );
    }
    if (pagingEnabled) {
      int pageSize = column.getPageSize();
      boundStatement = boundStatement.setFetchSize(pageSize);
    }
    ResultSetFuture res = admin.executeAsync(boundStatement);
    resultSetFutures.add(res);
  }

  private int queryCassandraSingleColumnAndUpdateFuturesScan(
      CassandraAdmin admin,
      String cassandraTableName,
      String translatedLocalityGroup,
      String translatedFamily,
      String translatedQualifier,
      Long minTimestamp,
      Long maxTimestamp,
      KijiDataRequest.Column column,
      HashSet<ResultSetFuture> resultSetFutures) {
    int numScanQueries = 0;

    if (translatedQualifier != null) {
      // Fully-qualified scan.
      // Note - we cannot use "LIMIT" in this Cassandra query because we need to perform this
      // query across multiple rows.
      String queryString = String.format(
          "SELECT token(%s), %s, %s, %s, %s, %s, %s FROM %s WHERE %s=? AND %s=? AND %s=? ALLOW FILTERING",
          CassandraKiji.CASSANDRA_KEY_COL,
          CassandraKiji.CASSANDRA_KEY_COL,
          CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
          CassandraKiji.CASSANDRA_FAMILY_COL,
          CassandraKiji.CASSANDRA_QUALIFIER_COL,
          CassandraKiji.CASSANDRA_VERSION_COL,
          CassandraKiji.CASSANDRA_VALUE_COL,
          cassandraTableName,
          CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
          CassandraKiji.CASSANDRA_FAMILY_COL,
          CassandraKiji.CASSANDRA_QUALIFIER_COL
      );
      LOG.info("Preparing query string " + queryString);
      PreparedStatement preparedStatement = admin.getPreparedStatement(queryString);
      ResultSetFuture res = admin.executeAsync(preparedStatement.bind(
          translatedLocalityGroup,
          translatedFamily,
          translatedQualifier));
      resultSetFutures.add(res);
      numScanQueries++;
    } else if (translatedQualifier == null) {
      String queryString = String.format(
          "SELECT token(%s), %s, %s, %s, %s, %s, %s FROM %s WHERE %s=? AND %s=? ALLOW FILTERING",
          CassandraKiji.CASSANDRA_KEY_COL,
          CassandraKiji.CASSANDRA_KEY_COL,
          CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
          CassandraKiji.CASSANDRA_FAMILY_COL,
          CassandraKiji.CASSANDRA_QUALIFIER_COL,
          CassandraKiji.CASSANDRA_VERSION_COL,
          CassandraKiji.CASSANDRA_VALUE_COL,
          cassandraTableName,
          CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
          CassandraKiji.CASSANDRA_FAMILY_COL
      );
      LOG.info("Preparing query string " + queryString);
      PreparedStatement preparedStatement = admin.getPreparedStatement(queryString);
      ResultSetFuture res = admin.executeAsync(preparedStatement.bind(translatedLocalityGroup, translatedFamily));
      resultSetFutures.add(res);
      numScanQueries++;

    }
    return numScanQueries;
  }
}
