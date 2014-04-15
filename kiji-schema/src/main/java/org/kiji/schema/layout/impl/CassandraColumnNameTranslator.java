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

package org.kiji.schema.layout.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;

/**
 * Translates Kiji column names into shorter families and qualifiers.
 *
 * TODO: Update Cassandra column name translators with short, identity, native.
 *
 */
@ApiAudience.Private
public final class CassandraColumnNameTranslator extends ShortColumnNameTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraColumnNameTranslator.class);

  /**
   * Creates a new <code>CassandraColumnNameTranslator</code> instance.
   *
   * @param tableLayout The layout of the table to translate column names for.
   */
  public CassandraColumnNameTranslator(KijiTableLayout tableLayout) {
    super(tableLayout);
  }

  /**
   * Get the translated name of the locality group of a column for Cassandra.
   *
   * @param kijiColumnName to translate.
   * @return the locality group name (translated for Cassandra).
   * @throws NoSuchColumnException if the column does not exist.
   */
  public String toCassandraLocalityGroup(
      KijiColumnName kijiColumnName) throws NoSuchColumnException {
    final String familyName = kijiColumnName.getFamily();
    final FamilyLayout familyLayout = mTableLayout.getFamilyMap().get(familyName);
    if (null == familyLayout) {
      throw new NoSuchColumnException(kijiColumnName.toString());
    }
    final ColumnId localityGroupId = familyLayout.getLocalityGroup().getId();
    return localityGroupId.toString();
  }

  /**
   * Get name of column family (for Cassandra) for this column.
   *
   * @param kijiColumnName to translate.
   * @return the column family name (translated for Cassandra).
   * @throws NoSuchColumnException if the column does not exist.
   */
  public String toCassandraColumnFamily(
      KijiColumnName kijiColumnName) throws NoSuchColumnException {
    final String familyName = kijiColumnName.getFamily();
    final FamilyLayout familyLayout = mTableLayout.getFamilyMap().get(familyName);
    if (null == familyLayout) {
      throw new NoSuchColumnException(kijiColumnName.toString());
    }
    //final ColumnId localityGroupId = familyLayout.getLocalityGroup().getId();
    final ColumnId familyId = familyLayout.getId();

    return familyId.toString();
  }

  /**
   * Get the name of the column qualifier, translated for Cassandra, for this column.
   *
   * @param kijiColumnName to translate.
   * @return the name of the column qualifier, translated for Cassandra.
   * @throws NoSuchColumnException if the column does not exist.
   */
  public String toCassandraColumnQualifier(
      KijiColumnName kijiColumnName) throws NoSuchColumnException {
    // Check that the family exists before getting to the qualifier!
    final String familyName = kijiColumnName.getFamily();
    final FamilyLayout familyLayout = mTableLayout.getFamilyMap().get(familyName);
    if (null == familyLayout) {
      throw new NoSuchColumnException(kijiColumnName.toString());
    }

    final String qualifier = kijiColumnName.getQualifier();
    if (familyLayout.isGroupType()) {
      // Group type family.
      if (null != qualifier) {
        // Qualifier is specified.
        final ColumnId columnId = familyLayout.getColumnIdNameMap().inverse().get(qualifier);
        if (null == columnId) {
          throw new NoSuchColumnException(kijiColumnName.toString());
        }
        return columnId.toString();
      }

      // Group-type, but qualifier is null.
      //assert(familyLayout.isGroupType() && null == qualifier);

      // The caller is attempting to translate a Kiji column name that has only a family,
      // no qualifier.  This is okay.  We'll just return an HBaseColumnName with an empty
      // qualifier suffix.
      return null;
    } else {
      // Map type family - Don't do any translation on the column name.
      assert familyLayout.isMapType();
      return qualifier;
    }
  }

  /**
   * Create the Kiji column name, given the Cassandra versions of the locality group, family, and
   * qualifier.
   *
   * @param cassandraLocalityGroup for the column.
   * @param cassandraColumnFamily for the column.
   * @param cassandraColumnQualifier for the column.
   * @return The Kiji column name.
   * @throws NoSuchColumnException if the column does not exist.
   */
  public KijiColumnName toKijiColumnName(
      String cassandraLocalityGroup,
      String cassandraColumnFamily,
      String cassandraColumnQualifier) throws NoSuchColumnException {

    LOG.debug(String.format(
        "Translating C* column name family='%s', qual='%s' to Kiji column name...",
        cassandraColumnFamily,
        cassandraColumnQualifier));

    // Get the locality group and the column family out of the
    ColumnId localityGroupId = ColumnId.fromString(cassandraLocalityGroup);
    ColumnId familyId = ColumnId.fromString(cassandraColumnFamily);
    ColumnId qualifierId = ColumnId.fromString(cassandraColumnQualifier);

    final KijiTableLayout.LocalityGroupLayout localityGroup = mLocalityGroups.get(localityGroupId);
      if (null == localityGroup) {
        // TODO: Real error message.
        throw new NoSuchColumnException("oops!");
      }

    final FamilyLayout kijiFamily = getKijiFamilyById(localityGroup, familyId);
    if (null == kijiFamily) {
      throw new NoSuchColumnException(String.format(
          "No family with ColumnId '%s' in locality group '%s'.",
          familyId, localityGroup.getDesc().getName()));
    }

    if (kijiFamily.isGroupType()) {
      // Group type family.
      final FamilyLayout.ColumnLayout kijiColumn = getKijiColumnById(kijiFamily, qualifierId);
      if (null == kijiColumn) {
        throw new NoSuchColumnException(String.format(
            "No column with ColumnId '%s' in family '%s'.",
            qualifierId, kijiFamily.getDesc().getName()));
      }
      final KijiColumnName result =
          new KijiColumnName(kijiFamily.getDesc().getName(), kijiColumn.getDesc().getName());
      LOG.debug(String.format("Translated to Kiji group column '%s'.", result));
      return result;
    }

      // Map type family.
      assert kijiFamily.isMapType();
      final KijiColumnName result =
          new KijiColumnName(kijiFamily.getDesc().getName(), cassandraColumnQualifier);
      LOG.debug(String.format("Translated to Kiji map column '%s'.", result));
      return result;
  }
}
