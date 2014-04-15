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

package org.kiji.schema.layout.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiCellDecoder;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.SpecificCellDecoderFactory;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.avro.TableLayoutBackupEntry;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.avro.TableLayoutsBackup;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.impl.AvroCellEncoder;
import org.kiji.schema.impl.cassandra.CassandraAdmin;
import org.kiji.schema.impl.cassandra.CassandraByteUtil;
import org.kiji.schema.impl.cassandra.CassandraTableInterface;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayoutDatabase;
import org.kiji.schema.layout.TableLayoutBuilder;
import org.kiji.schema.layout.TableLayoutBuilder.LayoutOptions;
import org.kiji.schema.layout.TableLayoutBuilder.LayoutOptions.SchemaFormat;
import org.kiji.schema.platform.SchemaPlatformBridge;

/**
 * <p>Manages Kiji table layouts using a Cassandra table as a backing store.</p>
 *
 * <p>
 * The C* primary key is the name of the table, and the row has 3 columns:
 *   <li> timestamp (needs to be explicit in C*);</li>
 *   <li> the layout update, as specified by the user/submitter; </li>
 *   <li> the effective layout after applying the update; </li>
 *   <li> a hash of the effective layout. </li>
 * </p>
 *
 * <p>
 * Layouts and layout updates are encoded as Kiji cells, using Avro schema hashes, and as
 * TableLayoutDesc Avro records.
 * </p>
 *
 * <p>A static method, <code>getHColumnDescriptor</code> returns the description of an
 * HColumn that should be used to construct the HTable for the backing store.</p>
 */
@ApiAudience.Private
public final class CassandraTableLayoutDatabase implements KijiTableLayoutDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraTableLayoutDatabase.class);

  public static final String QUALIFIER_TABLE = "table_name";
  public static final String QUALIFIER_TIME = "time";

  /**
   * C* column used to store layout updates.
   * Layout updates are binary encoded TableLayoutDesc records.
   */
  public static final String QUALIFIER_UPDATE = "layout_update";

  /**
   * C* column used to store absolute layouts.
   * Table layouts are binary encoded TableLayoutDesc records.
   */
  public static final String QUALIFIER_LAYOUT = "layout";

  /**
   * C* column used to store layout IDs.
   * Currently, IDs are assigned using a long counter starting at 1, and encoded as a string.
   */
  public static final String QUALIFIER_LAYOUT_ID = "layout_id";

  /** URI of the Kiji instance this layout database is for. */
  private final KijiURI mKijiURI;

  /** The C* table to use to store the layouts. */
  private final CassandraTableInterface mTable;

  /** Cassandra administration object used for sending CQL requests to C* cluster. */
  private final CassandraAdmin mAdmin;

  /** The schema table. */
  private final KijiSchemaTable mSchemaTable;

  /** Kiji cell encoder. */
  private final KijiCellEncoder mCellEncoder;

  /** Decoder for concrete layout cells. */
  private final KijiCellDecoder<TableLayoutDesc> mCellDecoder;

  private PreparedStatement mPreparedStatementGetRows = null;
  private PreparedStatement mPreparedStatementUpdateTableLayout = null;
  private PreparedStatement mPreparedStatementRemoveAllTableLayoutVersions = null;
  private PreparedStatement mPreparedStatementRemoveRecentTableLayoutVersions = null;
  private PreparedStatement mPreparedStatementListTables = null;

  private static final CellSchema CELL_SCHEMA = CellSchema.newBuilder()
      .setStorage(SchemaStorage.HASH)
      .setType(SchemaType.CLASS)
      .setValue(TableLayoutDesc.SCHEMA$.getFullName())
      .build();

  // TODO: Don't need to separate statement preparation after adding separate statement cache.
  /** Prepare statement. */
  private void setPreparedStatementGetRows() {
    String metaTableName = mTable.getTableName();

    String queryText = String.format(
        "SELECT * FROM %s WHERE %s=? LIMIT 1",
        metaTableName,
        QUALIFIER_TABLE
    );
    mPreparedStatementGetRows = mAdmin.getPreparedStatement(queryText);
  }

  // TODO: Don't need to separate statement preparation after adding separate statement cache.
  /** Prepare statement. */
  private void setPreparedStatementUpdateTableLayout() {
    String metaTableName = mTable.getTableName();

    String queryText = String.format(
        "INSERT INTO %s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?)",
        metaTableName,
        QUALIFIER_TABLE,
        QUALIFIER_TIME,
        QUALIFIER_LAYOUT_ID,
        QUALIFIER_LAYOUT,
        QUALIFIER_UPDATE);
    mPreparedStatementUpdateTableLayout = mAdmin.getPreparedStatement(queryText);
  }

  // TODO: Don't need to separate statement preparation after adding separate statement cache.
  /** Prepare statement. */
  private void setPreparedStatementRemoveAllTableLayoutVersions() {
    String metaTableName = mTable.getTableName();
    String queryText = String.format(
        "DELETE FROM %s WHERE %s=?",
        metaTableName,
        QUALIFIER_TABLE
      );
    mPreparedStatementRemoveAllTableLayoutVersions = mAdmin.getPreparedStatement(queryText);
  }

  // TODO: Don't need to separate statement preparation after adding separate statement cache.
  /** Prepare statement. */
  private void setPreparedStatementRemoveRecentTableLayoutVersions() {
    String metaTableName = mTable.getTableName();
    String queryText = String.format(
        "DELETE FROM %s WHERE %s=? AND %s=?",
        metaTableName,
        QUALIFIER_TABLE,
        QUALIFIER_TIME
    );
    mPreparedStatementRemoveRecentTableLayoutVersions = mAdmin.getPreparedStatement(queryText);
  }

  // TODO: Don't need to separate statement preparation after adding separate statement cache.
  /** Prepare statement. */
  private void setPreparedStatementListTables() {
    String queryText = String.format(
        "SELECT %s FROM %s",
        QUALIFIER_TABLE,
        mTable.getTableName()
    );
    mPreparedStatementListTables = mAdmin.getPreparedStatement(queryText);
  }
  /**
   * Install a table for storing table layout information.
   * @param admin A wrapper around an open C* session.
   * @param uri The KijiURI of the instance for this table.
   */
  public static void install(CassandraAdmin admin, KijiURI uri) {
    String tableName = KijiManagedCassandraTableName.getMetaLayoutTableName(uri).toString();

    // Standard C* table layout.  Use text key + timestamp as composite primary key to allow
    // selection by timestamp.

    // I did not use timeuuid here because we need to be able to write timestamps sometimes.

    // For the rest of the table, the layout ID is a string, then we store the actual layout and
    // update as blobs.
    String tableDescription = String.format(
        "CREATE TABLE %s (%s text, %s timestamp, %s text, %s blob, %s blob, PRIMARY KEY (%s, %s)) "
            + "WITH CLUSTERING ORDER BY (%s DESC);",
        tableName,
        QUALIFIER_TABLE,
        QUALIFIER_TIME,
        QUALIFIER_LAYOUT_ID,
        QUALIFIER_LAYOUT,
        QUALIFIER_UPDATE,
        QUALIFIER_TABLE,
        QUALIFIER_TIME,
        QUALIFIER_TIME);
    admin.createTable(tableName, tableDescription);
  }

  /**
   * Creates a new <code>CassandraTableLayoutDatabase</code> instance.
   *
   * <p>This class does not take ownership of the table.  The caller should close it when
   * it is no longer needed.</p>
   *
   * @param kijiURI URI of the Kiji instance this layout database belongs to.
   * @param ctable The C* table used to store the layout data.
   * @param schemaTable The Kiji schema table.
   * @throws java.io.IOException on I/O error.
   */
  public CassandraTableLayoutDatabase(
      KijiURI kijiURI,
      CassandraTableInterface ctable,
      KijiSchemaTable schemaTable)
      throws IOException {
    mKijiURI = kijiURI;
    mTable = Preconditions.checkNotNull(ctable);
    mAdmin = mTable.getAdmin();
    mSchemaTable = Preconditions.checkNotNull(schemaTable);
    final CellSpec cellSpec = CellSpec.fromCellSchema(CELL_SCHEMA, mSchemaTable);
    mCellEncoder = new AvroCellEncoder(cellSpec);
    mCellDecoder = SpecificCellDecoderFactory.get().create(cellSpec);
    setPreparedStatementGetRows();
    setPreparedStatementRemoveAllTableLayoutVersions();
    setPreparedStatementUpdateTableLayout();
    setPreparedStatementRemoveRecentTableLayoutVersions();
    setPreparedStatementListTables();
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout updateTableLayout(String tableName, TableLayoutDesc layoutUpdate)
      throws IOException {

    // Normalize the new layout to use schema UIDs:
    TableLayoutBuilder layoutBuilder = new TableLayoutBuilder(mSchemaTable);
    final TableLayoutDesc update = layoutBuilder.normalizeTableLayoutDesc(
        layoutUpdate,
        new LayoutOptions()
            .setSchemaFormat(SchemaFormat.UID));

    // Fetch all the layout history:
    final List<KijiTableLayout> layouts =
        getTableLayoutVersions(tableName, HConstants.ALL_VERSIONS);
    final KijiTableLayout currentLayout = layouts.isEmpty() ? null : layouts.get(0);
    final KijiTableLayout tableLayout = KijiTableLayout.createUpdatedLayout(update, currentLayout);

    Preconditions.checkArgument(tableName.equals(tableLayout.getName()));

    // Set of all the former layout IDs:
    final Set<String> layoutIDs = Sets.newHashSet();
    for (KijiTableLayout layout : layouts) {
      layoutIDs.add(layout.getDesc().getLayoutId());
    }

    final String refLayoutIdStr = update.getReferenceLayout();

    final boolean hasCurrentLayout = (null != currentLayout);
    final boolean hasRefLayoutId = (null != refLayoutIdStr);
    if (hasCurrentLayout && !hasRefLayoutId) {
      throw new IOException(String.format(
          "Layout for table '%s' does not specify reference layout ID.", tableName));
    }
    if (!hasCurrentLayout && hasRefLayoutId) {
      throw new IOException(String.format(
          "Initial layout for table '%s' must not specify reference layout ID.", tableName));
    }

    final String layoutId = tableLayout.getDesc().getLayoutId();

    if (layoutIDs.contains(layoutId)) {
      throw new InvalidLayoutException(tableLayout,
          String.format("Layout ID '%s' already exists", layoutId));
    }

    //String metaTableName = mTable.getTableName();

    Preconditions.checkNotNull(mPreparedStatementUpdateTableLayout);
    // TODO: This should do a "check-and-put" to match the HBase implementation.
    mAdmin.execute(mPreparedStatementUpdateTableLayout.bind(
        tableName,
        new Date(),
        layoutId,
        CassandraByteUtil.bytesToByteBuffer(encodeTableLayoutDesc(tableLayout.getDesc())),
        CassandraByteUtil.bytesToByteBuffer(encodeTableLayoutDesc(update))
    ));

    // TODO: C* flush?
    // Flush the writer schema for the Avro table layout first so other readers can see it.
    //mSchemaTable.flush();

    return tableLayout;
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableLayout getTableLayout(String table) throws IOException {
    final List<KijiTableLayout> layouts = getTableLayoutVersions(table, 1);
    if (layouts.isEmpty()) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mKijiURI).withTableName(table).build());
    }
    return layouts.get(0);
  }

  /**
   * Internal helper method containing common code for ready values for a given key from the table.
   * @param table Name of the table for which to fetch the values (part of the key-value database
   *              key).
   * @param numVersions Number of versions to fetch for the given table, key combination.
   * @return A list of C* rows for the query.
   */
  private List<Row> getRows(String table, int numVersions) {
    Preconditions.checkArgument(numVersions >= 1,  "numVersions must be positive");
    Preconditions.checkNotNull(mPreparedStatementGetRows);
    ResultSet resultSet = mAdmin.execute(mPreparedStatementGetRows.bind(table));
    return resultSet.all();
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiTableLayout> getTableLayoutVersions(String table, int numVersions)
      throws IOException {
    Preconditions.checkArgument(numVersions >= 1,  "numVersions must be positive");

    List<Row> rows = getRows(table, numVersions);

    // Convert result into a list of bytes
    final List<KijiTableLayout> layouts = Lists.newArrayList();
    for (Row row: rows) {
      ByteBuffer blob = row.getBytes(QUALIFIER_LAYOUT);
      byte[] bytes = CassandraByteUtil.byteBuffertoBytes(blob);
      layouts.add(KijiTableLayout.newLayout(decodeTableLayoutDesc(bytes)));
    }
    return layouts;
  }

  /** {@inheritDoc} */
  @Override
  public NavigableMap<Long, KijiTableLayout> getTimedTableLayoutVersions(
      String table, int numVersions) throws IOException {
    Preconditions.checkArgument(numVersions >= 1, "numVersions must be positive");

    List<Row> rows = getRows(table, numVersions);

    // Convert result into a map from timestamps to values
    final NavigableMap<Long, KijiTableLayout> timedValues = Maps.newTreeMap();
    for (Row row: rows) {
      ByteBuffer blob = row.getBytes(QUALIFIER_LAYOUT);
      byte[] bytes = CassandraByteUtil.byteBuffertoBytes(blob);
      KijiTableLayout layout = KijiTableLayout.newLayout(decodeTableLayoutDesc(bytes));

      Long timestamp = row.getDate(QUALIFIER_TIME).getTime();
      Preconditions.checkState(timedValues.put(timestamp, layout) == null);
    }
    return timedValues;
  }

  /** {@inheritDoc} */
  @Override
  public void removeAllTableLayoutVersions(String table) throws IOException {
    // TODO: Check for success?
    Preconditions.checkNotNull(mPreparedStatementRemoveAllTableLayoutVersions);
    mAdmin.execute(mPreparedStatementRemoveAllTableLayoutVersions.bind(table));

  }

  /** {@inheritDoc} */
  @Override
  public void removeRecentTableLayoutVersions(String table, int numVersions) throws IOException {
    Preconditions.checkArgument(numVersions >= 1, "numVersions must be positive");
    // Unclear how to do this in C* without first reading about the most-recent versions

    // Get a list of versions to delete
    List<Row> rows = getRows(table, numVersions);

    Preconditions.checkNotNull(mPreparedStatementRemoveRecentTableLayoutVersions);
    for (Row row: rows) {
      Long timestamp = row.getDate(QUALIFIER_TIME).getTime();
      mAdmin.execute(
          mPreparedStatementRemoveRecentTableLayoutVersions.bind(table, new Date(timestamp)));
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listTables() throws IOException {
    Preconditions.checkNotNull(mPreparedStatementListTables);


    // Just return a set of in-use tables
    ResultSet resultSet = mAdmin.execute(mPreparedStatementListTables.bind());
    Set<String> keys = new HashSet<String>();

    // This code makes me miss Scala
    for (Row row: resultSet.all()) {
      keys.add(row.getString(QUALIFIER_TABLE));
    }

    List<String> list = new ArrayList<String>();
    list.addAll(keys);
    return list;
  }

  /** {@inheritDoc} */
  @Override
  public boolean tableExists(String tableName) throws IOException {
    List<String> tables = listTables();
    return tables.contains(tableName);
  }

  /**
   * Gets the description of an HColumn suitable for storing the table layout database.
   *
   * @param family The family within the HTable used to store layout data.
   * @return The HColumn descriptor.
   */
  public static HColumnDescriptor getHColumnDescriptor(String family) {
    return SchemaPlatformBridge.get().createHColumnDescriptorBuilder(Bytes.toBytes(family))
        .setMaxVersions(HConstants.ALL_VERSIONS)
        .setCompressionType("none")
        .setInMemory(false)
        .setBlockCacheEnabled(true)
        .setTimeToLive(HConstants.FOREVER)
        .setBloomType("NONE")
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public TableLayoutsBackup layoutsToBackup(String table) throws IOException {
    final List<TableLayoutBackupEntry> history = Lists.newArrayList();
    TableLayoutsBackup backup = TableLayoutsBackup.newBuilder().setLayouts(history).build();

    List<Row> rows = getRows(table, Integer.MAX_VALUE);
    if (rows.isEmpty()) {
      LOG.info(String.format(
          "There is no row in the MetaTable named '%s' or the row is empty.", table));
      return backup;
    }

    for (Row row: rows) {
      final long timestamp = row.getDate(QUALIFIER_TIME).getTime();
      final TableLayoutDesc layout =
          decodeTableLayoutDesc(
              CassandraByteUtil.byteBuffertoBytes(
                row.getBytes(QUALIFIER_LAYOUT)
              )
          );


      // TODO: May need some check here that the update is not null
      final TableLayoutDesc update =
          decodeTableLayoutDesc(
              CassandraByteUtil.byteBuffertoBytes(
                  row.getBytes(QUALIFIER_UPDATE)
              )
          );

      history.add(TableLayoutBackupEntry.newBuilder()
          .setLayout(layout)
          .setUpdate(update)
          .setTimestamp(timestamp)
          .build());
    }
    return backup;
  }

  /** {@inheritDoc} */
  @Override
  public void restoreLayoutsFromBackup(String tableName, TableLayoutsBackup layoutBackup) throws
      IOException {
    LOG.info(String.format("Restoring layout history for table '%s'.", tableName));
    String metaTableName = mTable.getTableName();

    // Looks like we need insertions with and without updates and timestamps.

    // TODO: Make this query a member of the class and prepare in the constructor
    String queryTextInsertAll = String.format(
        "INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
        metaTableName,
        QUALIFIER_TABLE,
        QUALIFIER_TIME,
        QUALIFIER_LAYOUT,
        QUALIFIER_UPDATE);
    PreparedStatement preparedStatementInsertAll = mAdmin.getPreparedStatement(queryTextInsertAll);

    String queryTextInsertLayout = String.format(
        "INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
        metaTableName,
        QUALIFIER_TABLE,
        QUALIFIER_TIME,
        QUALIFIER_LAYOUT);
    PreparedStatement preparedStatementInsertLayout =
        mAdmin.getPreparedStatement(queryTextInsertLayout);

    // TODO: Unclear what happens to layout IDs here...
    for (TableLayoutBackupEntry lbe : layoutBackup.getLayouts()) {
      final byte[] layoutBytes = encodeTableLayoutDesc(lbe.getLayout());
      final ByteBuffer layoutByteBuffer = CassandraByteUtil.bytesToByteBuffer(layoutBytes);

      if (lbe.getUpdate() != null) {
        final byte[] updateBytes = encodeTableLayoutDesc(lbe.getUpdate());
        final ByteBuffer updateByteBuffer = CassandraByteUtil.bytesToByteBuffer(updateBytes);
        final long timestamp = lbe.getTimestamp();

        mAdmin.execute(preparedStatementInsertAll.bind(
            tableName,
            new Date(timestamp),
            layoutByteBuffer,
            updateByteBuffer
        ));
      } else {
        mAdmin.execute(preparedStatementInsertLayout.bind(
            tableName,
            new Date(),
            layoutByteBuffer
        ));
      }
    }

    // TODO: Some kind of flush?

  }

  /**
   * Decodes a table layout descriptor from binary.
   *
   * @param bytes Serialized table layout descriptor.
   * @return Deserialized table layout descriptor.
   * @throws java.io.IOException on I/O or decoding error.
   */
  private TableLayoutDesc decodeTableLayoutDesc(byte[] bytes) throws IOException {
    return mCellDecoder.decodeValue(bytes);
  }

  /**
   * Encodes a table layout descriptor to binary.
   *
   * @param desc Table layout descriptor to serialize.
   * @return Table layout descriptor encoded as bytes.
   * @throws java.io.IOException on I/O or encoding error.
   */
  private byte[] encodeTableLayoutDesc(TableLayoutDesc desc) throws IOException {
    return mCellEncoder.encode(desc);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraTableLayoutDatabase.class)
        .add("uri", mKijiURI)
        .toString();
  }
}
