package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCellEncoder;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;
import org.kiji.schema.impl.cassandra.CassandraKijiTableWriter.WriterLayoutCapsule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Contains code common to a TableWriter and BufferedWriter.
 */
class CassandraKijiWriterCommon {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiWriterCommon.class);

  private final CassandraAdmin mAdmin;

  private final CassandraKijiTable mTable;

  private volatile WriterLayoutCapsule mWriterLayoutCapsule;

  private final String mTableName;

  private final String mCounterTableName;

  public CassandraKijiWriterCommon(
      CassandraKijiTable table,
      CassandraKijiTableWriter.WriterLayoutCapsule capsule) {
    mTable = table;
    mWriterLayoutCapsule = capsule;
    mAdmin = mTable.getAdmin();
    mTableName = KijiManagedCassandraTableName
        .getKijiTableName(mTable.getURI(), mTable.getName()).toString();
    mCounterTableName = KijiManagedCassandraTableName
        .getKijiCounterTableName(mTable.getURI(), mTable.getName()).toString();
  }

  public boolean isCounterColumn(String family, String qualifier) throws IOException {
    return mWriterLayoutCapsule
        .getLayout()
        .getCellSpec(new KijiColumnName(family, qualifier))
        .isCounter();
  }
  public boolean isCounterColumn(String family) throws IOException {
    return mWriterLayoutCapsule
        .getLayout()
        .getCellSpec(new KijiColumnName(family))
        .isCounter();
  }

  // Get the Statement for a put to a non-counter cell.
  public<T> Statement getStatementPutNotCounter(
      EntityId entityId,
      String family,
      String qualifier,
      long timestamp,
      T value) throws IOException {
    final CassandraKijiTableWriter.WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    Preconditions.checkArgument(!isCounterColumn(family, qualifier));

    // Encode the entity ID and value as ByteBuffers (Cassandra "blobs").
    final KijiCellEncoder cellEncoder =
        capsule.getCellEncoderProvider().getEncoder(family, qualifier);

    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());
    final byte[] encoded = cellEncoder.encode(value);
    final ByteBuffer encodedByteBuffer = CassandraByteUtil.bytesToByteBuffer(encoded);

    // TODO: Refactor this query text (and preparation for it) elsewhere.
    // Create the CQL statement to insert data.
    String queryText = String.format(
        "INSERT INTO %s (%s, %s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?, ?);",
        mTableName,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL,
        CassandraKiji.CASSANDRA_VALUE_COL);
    LOG.info(queryText);

    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) capsule.getColumnNameTranslator();

    PreparedStatement preparedStatement = mAdmin.getPreparedStatement(queryText);
    return preparedStatement.bind(
        rowKey,
        translator.toCassandraLocalityGroup(columnName),
        translator.toCassandraColumnFamily(columnName),
        translator.toCassandraColumnQualifier(columnName),
        timestamp,
        encodedByteBuffer
    );
  }

  public Statement getStatementDeleteCell(
      EntityId entityId, String family, String qualifier, long timestamp) throws IOException {
    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout familyLayout = capsule.getLayout().getFamilyMap().get(family);
    if (null == familyLayout) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }

    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) capsule.getColumnNameTranslator();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);

    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    String tableName = isCounterColumn(family, qualifier) ? mCounterTableName : mTableName;

    // TODO: Prepare this statement first.
    String queryString = String.format(
        "DELETE FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=? AND %s=?",
        tableName,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL,
        CassandraKiji.CASSANDRA_VERSION_COL
    );

    PreparedStatement preparedStatement = mAdmin.getPreparedStatement(queryString);
    return preparedStatement.bind(
        rowKey,
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName),
        translator.toCassandraColumnQualifier(kijiColumnName),
        timestamp
    );
  }

  public Statement getStatementDeleteColumn(
      EntityId entityId, String family, String qualifier) throws IOException {
    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout familyLayout = capsule.getLayout().getFamilyMap().get(family);
    if (null == familyLayout) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }

    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) capsule.getColumnNameTranslator();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);

    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    String tableName = isCounterColumn(family, qualifier) ? mCounterTableName : mTableName;

    // TODO: Prepare this statement first.
    String queryString = String.format(
        "DELETE FROM %s WHERE %s=? AND %s=? AND %s=? AND %s=?",
        tableName,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL,
        CassandraKiji.CASSANDRA_QUALIFIER_COL
    );

    PreparedStatement preparedStatement = mAdmin.getPreparedStatement(queryString);
    return preparedStatement.bind(
        rowKey,
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName),
        translator.toCassandraColumnQualifier(kijiColumnName)
    );
  }

  /**
   * Does the non-counter deletes.
   * @param entityId
   * @param family
   * @return
   * @throws IOException
   */
  public Statement getStatementDeleteFamily(EntityId entityId, String family) throws IOException {
    return getStatementDeleteFamilyWithTableName(mTableName, entityId, family);
  }

  public Statement getStatementDeleteFamilyCounter(
      EntityId entityId,
      String family) throws IOException {
    return getStatementDeleteFamilyWithTableName(mCounterTableName, entityId, family);
  }

  /**
   * Common code for deleting counter and non-counter cells.
   *
   * @param tableName
   * @param entityId
   * @param family
   * @return
   * @throws IOException
   */
  private Statement getStatementDeleteFamilyWithTableName(
      String tableName,
      EntityId entityId,
      String family) throws IOException {
    final WriterLayoutCapsule capsule = mWriterLayoutCapsule;
    final KijiTableLayout.LocalityGroupLayout.FamilyLayout familyLayout =
        capsule.getLayout().getFamilyMap().get(family);
    if (null == familyLayout) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }

    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) capsule.getColumnNameTranslator();
    final KijiColumnName kijiColumnName = new KijiColumnName(family);

    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    // TODO: Prepare this statement first.
    String queryString = String.format(
        "DELETE FROM %s WHERE %s=? AND %s=? AND %s=?",
        tableName,
        CassandraKiji.CASSANDRA_KEY_COL,
        CassandraKiji.CASSANDRA_LOCALITY_GROUP_COL,
        CassandraKiji.CASSANDRA_FAMILY_COL
    );

    PreparedStatement preparedStatement = mAdmin.getPreparedStatement(queryString);
    return preparedStatement.bind(
        rowKey,
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName)
    );
  }

  public Statement getStatementDeleteRow(EntityId entityId) throws IOException {
    return getStatementDeleteRowWithTableName(mTableName, entityId);
  }

  public Statement getStatementDeleteRowCounter(EntityId entityId) throws IOException {
    return getStatementDeleteRowWithTableName(mCounterTableName, entityId);
  }

  private Statement getStatementDeleteRowWithTableName(
      String tableName,
      EntityId entityId) throws IOException {
    final ByteBuffer rowKey = CassandraByteUtil.bytesToByteBuffer(entityId.getHBaseRowKey());

    // TODO: Prepare this statement first.
    String queryString = String.format(
        "DELETE FROM %s WHERE %s=?",
        tableName,
        CassandraKiji.CASSANDRA_KEY_COL
    );

    PreparedStatement preparedStatement = mAdmin.getPreparedStatement(queryString);
    return preparedStatement.bind(rowKey);
  }

  // Get the Statement for an increment to a counter cell.

  // Do a put to a counter cell.





}
