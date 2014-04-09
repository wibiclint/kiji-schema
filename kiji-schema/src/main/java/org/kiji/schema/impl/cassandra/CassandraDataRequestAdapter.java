/**
 * (c) Copyright 2014 WibiData, Inc.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;

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
      KijiTableReader.KijiScannerOptions kijiScannerOptions
    ) throws IOException {
    // TODO: Do something with the scan options.
    return queryCassandraTables(table, null, false);
  }

  /**
   * Perform a Cassandra CQL "SELECT" statement (like an HBase Get).  Will ignore any columns with
   * paging enabled (use `doPagedGet` for requests with paging).
   *
   * @param table
   * @param entityId EntityID for the given get.
   * @return
   * @throws IOException
   */
  public List<ResultSet> doGet(
      CassandraKijiTable table,
      EntityId entityId
  ) throws IOException {
    return queryCassandraTables(table, entityId, false);
  }

  public List<ResultSet> doPagedGet(
      CassandraKijiTable table,
      EntityId entityId
  ) throws IOException {
    return queryCassandraTables(table, entityId, true);
  }

  /**
   * Query a Cassandra table for a get or for a scan.
   *
   * @param table The Cassandra Kiji table to scan.
   * @param entityId Make null if this is a scan over all entity IDs (not currently supporting entity ID ranges).
   * @param pagingEnabled If true, all columns in the data request should be paged.  If false, skip
   *                      any paged columns in the data request.
   * @return A list of results for the Cassandra query.
   * @throws IOException
   */
  private List<ResultSet> queryCassandraTables(
      CassandraKijiTable table,
      EntityId entityId,
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
    Set<ResultSetFuture> futures = Sets.newHashSet();

    // Timestamp limits for queries.
    long maxTimestamp = mKijiDataRequest.getMaxTimestamp();
    long minTimestamp = mKijiDataRequest.getMinTimestamp();

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

      // TODO: Optimize these queries such that we need only one RPC per column family.
      // (Right now a data request that asks for "info:foo" and "info:bar" would trigger two
      // separate session.execute(statement) commands.

      // Determine whether we need to read non-counter values and/or counter values.
      List<String> tableNames = Lists.newArrayList();

      if (maybeContainsNonCounterValues(table, kijiColumnName)) {
        tableNames.add(nonCounterTableName);
      }

      if (maybeContainsCounterValues(table, kijiColumnName)) {
        tableNames.add(counterTableName);
      }

      for (String cassandraTableName : tableNames) {
        if (bIsScan) {
          Statement statement = CQLUtils.getColumnScanStatement(
              admin,
              table.getLayout(),
              cassandraTableName,
              localityGroup,
              family,
              qualifier);
          if (pagingEnabled) {
            statement.setFetchSize(column.getPageSize());
          }
          futures.add(admin.executeAsync(statement));
        } else {
          Statement statement =
              CQLUtils.getColumnGetStatement(
                  admin,
                  table.getLayout(),
                  cassandraTableName,
                  entityId,
                  localityGroup,
                  family,
                  qualifier,
                  null,
                  qualifier == null ? null : minTimestamp,
                  qualifier == null ? null : maxTimestamp,
                  qualifier == null ? null : column.getMaxVersions());
          if (pagingEnabled) {
            statement.setFetchSize(column.getPageSize());
          }
          futures.add(admin.executeAsync(statement));
        }
      }
    }

    if (bIsScan && futures.isEmpty()) {
      // If this is a scan, you need to make sure that you execute at least one SELECT statement,
      // just to get back every entity ID.  If you do not do so, then a user could
      // create a scanner with a data request that has a single, paged column, and you would never
      // execute a SELECT query below (because we wait to execute paged SELECT queries) and so you
      // would never get an iterator back with any row keys at all!  Eek!

      // TODO: do we need to scan the counter table as well?
      futures.add(
          admin.executeAsync(
              CQLUtils.getEntityIDScanStatement(admin, table.getLayout(), nonCounterTableName)));
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
}
