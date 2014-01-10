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

package org.kiji.schema.cassandra;

import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.NotAKijiManagedTableException;

/**
 * <p>Multiple instances of Kiji can be installed on a single Cassandra cluster.  Within a Kiji
 * instance, several Cassandra tables are created to manage system, metadata, schemas, and
 * user-space tables.  This class represents the name of one of those Cassandra tables that are
 * created and managed by Kiji.  This class should only be used internally in Kiji modules, or by
 * framework application developers who need direct access to Cassandra tables managed by Kiji.</p>
 *
 * <p> The names of tables in Cassandra created and managed by Kiji are made of a list of delimited
 * components.  There are at least 3 components of a name:
 *
 * <ol>
 *   <li>
 *     Prefix: a literal string "kiji" used to mark that this table is managed by Kiji.
 *   </li>
 *   <li>
 *     KijiInstance: the name of kiji instance managing this table.
 *   </li>
 *   <li>
 *     Type: the type of table (system, schema, meta, table).
 *   </li>
 * </ol>
 *
 * If the type of the table is "table", then its name (the name users of Kiji would use to refer to
 * it) is the fourth and final component.
 * </p>
 *
 * <p>
 * For example, a Cassandra cluster might have the following tables:
 * <pre>
 * devices
 * kiji_default.meta
 * kiji_default.schema
 * kiji_default.schema_hash
 * kiji_default.schema_id
 * kiji_default.system
 * kiji_default.table_foo
 * kiji_default.table_bar
 * kiji_experimental.meta
 * kiji_experimental.schema
 * kiji_experimental.schema_hash
 * kiji_experimental.schema_id
 * kiji_experimental.system
 * kiji_experimental.table_baz
 * </pre>
 *
 * In this example, there is an Cassandra keyspace completely unrelated to Kiji called "devices."
 * There are two Kiji installations, one called "default" and another called "experimental."  Within
 * the "default" installation, there are two Kiji tables, "foo" and "bar."  Within the
 * "experimental" installation, there is a single Kiji table "baz."
 * </p>
 *
 * Note that Cassandra will not allow us to put "." into our keyspace and table names, so we have to use another character as a delimter.  I chose  underscore here.
 *
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class KijiManagedCassandraTableName {

  /** The first component of all Cassandra table names managed by Kiji. */
  public static final String KIJI_COMPONENT = "kiji";

  /** Regexp matching Kiji system tables. */
  public static final Pattern KIJI_SYSTEM_TABLES_REGEX =
      Pattern.compile("kiji[_](.*)[.](meta|system|schema_hash|schema_id)");

  /** The name component used for the Kiji meta table. */
  private static final String KIJI_META_COMPONENT = "meta";

  /** The name component used for the Kiji schema hash table. */
  private static final String KIJI_SCHEMA_HASH_COMPONENT = "schema_hash";

  /** The name component used for the Kiji schema IDs table. */
  private static final String KIJI_SCHEMA_ID_COMPONENT = "schema_id";

  /** The name component used for the Kiji system table. */
  private static final String KIJI_SYSTEM_COMPONENT = "system";

  /** The name component used for all user-space Kiji tables. */
  private static final String KIJI_TABLE_COMPONENT = "table";

  /** The Cassandra table name. */
  private final String mCassandraTableName;


  /** The Kiji instance name. */
  private final String mKijiInstanceName;

  /** The Kiji table name, or null if it is not a user-space Kiji table. */
  private final String mKijiTableName;

  /**
   * Constructs a Kiji-managed Cassandra table name.
   *
   * @param kijiInstanceName The kiji instance name.
   * @param type The type component of the Cassandra table name (meta, schema, system, table).
   */
  private KijiManagedCassandraTableName(String kijiInstanceName, String type) {
    mCassandraTableName = KIJI_COMPONENT + "_" + kijiInstanceName + "." + type;
    mKijiInstanceName = kijiInstanceName;
    mKijiTableName = null;
  }

  /**
   * Constructs a Kiji-managed Cassandra table name.
   *
   * @param kijiInstanceName The kiji instance name.
   * @param type The type component of the Cassandra table name.
   * @param kijiTableName The name of the user-space Kiji table.
   */
  private KijiManagedCassandraTableName(String kijiInstanceName, String type, String kijiTableName) {
    mCassandraTableName = KIJI_COMPONENT + "_" + kijiInstanceName + "." + type + "_" + kijiTableName;
    mKijiInstanceName = kijiInstanceName;
    mKijiTableName = kijiTableName;
  }

  /**
   * Constructs using a Cassandra table name.
   *
   * @param cassandraTableName The Cassandra HTable name.
   * @return A new kiji-managed Cassandra table name.
   * @throws NotAKijiManagedTableException If the Cassandra table is not managed by kiji.
   */
  /*
  // TODO: Rewrite this to split properly across _ and . using Java regular expressions.
  public static KijiManagedCassandraTableName get(String cassandraTableName)
      throws NotAKijiManagedTableException {
    // Split it into components.
    String[] components = StringUtils.splitPreserveAllTokens(
        cassandraTableName, Character.toString(DELIMITER), 4);

    // Make sure the first component is 'kiji'.
    if (!components[0].equals(KIJI_COMPONENT)) {
      throw new NotAKijiManagedTableException(
          hbaseTableName, "Doesn't start with kiji name component.");
    }

    if (components.length == 3) {
      // It's a managed kiji meta/schema/system table.
      return new KijiManagedCassandraTableName(components[1], components[2]);
    } else if (components.length == 4) {
      // It's a user-space kiji table.
      return new KijiManagedCassandraTableName(components[1], components[2], components[3]);
    } else {
      // Wrong number of components... must not be a kiji table.
      throw new NotAKijiManagedTableException(
          hbaseTableName, "Invalid number of name components.");
    }
  }   */

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji meta table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji meta table.
   */
  public static KijiManagedCassandraTableName getMetaTableName(String kijiInstanceName) {
    return new KijiManagedCassandraTableName(kijiInstanceName, KIJI_META_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji schema hash table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji schema hash table.
   */
  public static KijiManagedCassandraTableName getSchemaHashTableName(String kijiInstanceName) {
    return new KijiManagedCassandraTableName(kijiInstanceName, KIJI_SCHEMA_HASH_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji schema IDs table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji schema IDs table.
   */
  public static KijiManagedCassandraTableName getSchemaIdTableName(String kijiInstanceName) {
    return new KijiManagedCassandraTableName(kijiInstanceName, KIJI_SCHEMA_ID_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds the Kiji system table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @return The name of the Cassandra table used to store the Kiji system table.
   */
  public static KijiManagedCassandraTableName getSystemTableName(String kijiInstanceName) {
    return new KijiManagedCassandraTableName(kijiInstanceName, KIJI_SYSTEM_COMPONENT);
  }

  /**
   * Gets a new instance of a Kiji-managed Cassandra table that holds a user-space Kiji table.
   *
   * @param kijiInstanceName The name of the Kiji instance.
   * @param kijiTableName The name of the user-space Kiji table.
   * @return The name of the Cassandra table used to store the user-space Kiji table.
   */
  public static KijiManagedCassandraTableName getKijiTableName(
      String kijiInstanceName, String kijiTableName) {
    return new KijiManagedCassandraTableName(kijiInstanceName, KIJI_TABLE_COMPONENT, kijiTableName);
  }

  /**
   * Gets the name of the Kiji instance this named table belongs to.
   *
   * @return The name of the kiji instance.
   */
  public String getKijiInstanceName() {
    return mKijiInstanceName;
  }

  /**
   * Gets the name of the Kiji table.
   * A user defined kiji table named "foo" in the default kiji instance will be stored in Cassandra
   * with the KijiManaged name "kiji_default.table_foo".  This method will return only "foo".
   *
   * @return The name of the kiji table, or null if this is not a user-space Kiji table.
   */
  public String getKijiTableName() {
    return mKijiTableName;
  }

  /**
   * Gets the name of the Cassandra table that stores the data for this Kiji table.
   *
   * @return The Cassandra table name as a UTF-8 encoded byte array.
   */
  /*
  public byte[] toBytes() {
    return Bytes.toBytes(mCassandraTableName);
  } */

  @Override
  public String toString() {
    return mCassandraTableName;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiManagedCassandraTableName)) {
      return false;
    }
    return toString().equals(other.toString());
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
