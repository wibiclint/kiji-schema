package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;

/**
 * Contains code common to a TableWriter and BufferedWriter.
 */
class CassandraKijiWriterCommon {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiWriterCommon.class);

  private final CassandraAdmin mAdmin;

  private final CassandraKijiTable mTable;

  private final String mTableName;

  private final String mCounterTableName;

  public CassandraKijiWriterCommon(CassandraKijiTable table) {
    mTable = table;
    mAdmin = mTable.getAdmin();
    mTableName = KijiManagedCassandraTableName
        .getKijiTableName(mTable.getURI(), mTable.getName()).toString();
    mCounterTableName = KijiManagedCassandraTableName
        .getKijiCounterTableName(mTable.getURI(), mTable.getName()).toString();
  }

  public boolean isCounterColumn(String family, String qualifier) throws IOException {
    return mTable.getLayoutCapsule()
        .getLayout()
        .getCellSpec(new KijiColumnName(family, qualifier))
        .isCounter();
  }

  private static int getTTL(KijiTableLayout layout, String family) {
    // Get the locality group name from the column name.
    return layout
      .getFamilyMap()
      .get(family)
      .getLocalityGroup()
      .getDesc()
      .getTtlSeconds();
  }

  /**
   * Create a (bound) CQL statement that implements a Kiji put into a non-counter cell.
   *
   * @param entityId The entity ID of the destination cell.
   * @param family The column family of the destination cell.
   * @param qualifier The column qualifier of the destination cell.
   * @param timestamp The timestamp of the destination cell.
   * @param value The bytes to be written to the destination cell.
   * @return A CQL `Statement` that implements the put.
   * @throws IOException If something goes wrong (e.g., the column does not exist).
   */
  public <T> Statement getPutStatement(
      CellEncoderProvider encoderProvider,
      EntityId entityId,
      String family,
      String qualifier,
      long timestamp,
      T value) throws IOException {
    Preconditions.checkArgument(!isCounterColumn(family, qualifier));

    int ttl = getTTL(mTable.getLayout(), family);

    final KijiColumnName columnName = new KijiColumnName(family, qualifier);
    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) mTable.getColumnNameTranslator();

    final ByteBuffer valueBytes =
        CassandraByteUtil.bytesToByteBuffer(
            encoderProvider.getEncoder(family, qualifier).encode(value));

    return CQLUtils.getInsertStatement(
        mAdmin,
        mTable.getLayout(),
        mTableName,
        entityId,
        translator.toCassandraLocalityGroup(columnName),
        translator.toCassandraColumnFamily(columnName),
        translator.toCassandraColumnQualifier(columnName),
        timestamp,
        valueBytes,
        ttl);
  }

  public Statement getDeleteCellStatement(
      EntityId entityId,
      String family,
      String qualifier,
      long version
  ) throws IOException {
    checkFamily(family);

    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) mTable.getColumnNameTranslator();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);

    return CQLUtils.getDeleteCellStatement(
        mAdmin,
        mTable.getLayout(),
        mTableName,
        entityId,
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName),
        translator.toCassandraColumnQualifier(kijiColumnName),
        version);
  }

  public Statement getDeleteColumnStatement(EntityId entityId, String family, String qualifier)
      throws IOException {
    checkFamily(family);

    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) mTable.getColumnNameTranslator();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);

    return CQLUtils.getDeleteColumnStatement(
        mAdmin,
        mTable.getLayout(),
        mTableName,
        entityId,
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName),
        translator.toCassandraColumnQualifier(kijiColumnName));
  }

  public Statement getDeleteCounterStatement(
      EntityId entityId,
      String family,
      String qualifier
  ) throws IOException {
    checkFamily(family);

    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) mTable.getColumnNameTranslator();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, qualifier);

    return CQLUtils.getDeleteColumnStatement(
        mAdmin,
        mTable.getLayout(),
        mCounterTableName,
        entityId,
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName),
        translator.toCassandraColumnQualifier(kijiColumnName));
  }

  public Statement getDeleteFamilyStatement(EntityId entityId, String family) throws IOException {
    checkFamily(family);

    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) mTable.getColumnNameTranslator();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, null);

    return CQLUtils.getDeleteFamilyStatement(
        mAdmin,
        mTable.getLayout(),
        mTableName,
        entityId,
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName));
  }


  public Statement getDeleteCounterFamilyStatement(EntityId entityId, String family)
      throws IOException {
    checkFamily(family);

    final CassandraColumnNameTranslator translator =
        (CassandraColumnNameTranslator) mTable.getColumnNameTranslator();
    final KijiColumnName kijiColumnName = new KijiColumnName(family, null);

    return CQLUtils.getDeleteFamilyStatement(
        mAdmin,
        mTable.getLayout(),
        mCounterTableName,
        entityId,
        translator.toCassandraLocalityGroup(kijiColumnName),
        translator.toCassandraColumnFamily(kijiColumnName));
  }

  public Statement getDeleteRowStatement(EntityId entityId) throws IOException {
    return CQLUtils.getDeleteRowStatement(mAdmin, mTable.getLayout(), mTableName, entityId);
  }

  public Statement getDeleteCounterRowStatement(EntityId entityId) throws IOException {
    return CQLUtils.getDeleteRowStatement(mAdmin, mTable.getLayout(), mCounterTableName, entityId);
  }

  /**
   * Checks that the provided column family exists in this table.
   *
   * @param family to check.
   * @throws NoSuchColumnException if the family does not exist.
   */
  private void checkFamily(String family) throws NoSuchColumnException {
    if (!mTable.getLayout().getFamilyMap().containsKey(family)) {
      throw new NoSuchColumnException(String.format("Family '%s' not found.", family));
    }
  }
}
