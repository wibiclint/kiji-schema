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

/**
 * Provides interface somewhat like HTableInterface, but for C*.
 *
 * We may want to refactor this out later.
 *
 * The DataStax Java driver has no notion of a pointer to a table object.  Everything instead goes
 * through a Session.  So here we'll instead just keep a pointer to a session and a table name.
 *
 * We may want to get rid of this object eventually.  For now, it just has getters for the
 * CassandraAdmin and table name.
 *
 * This class may be useful for managing how different objects can access tables, what actions they
 * can perform, and for keeping track of references to tables (for figuring out when to close open
 * Cassandra Sessions).
 *
 */
public final class CassandraTableInterface {

  /** C* Admin with open session. */
  private final CassandraAdmin mAdmin;

  /** Name of the table. */
  private final String mTableName;

  /**
   * Get the CassandraAdmin for this table.
   *
   * @return the admin.
   */
  public CassandraAdmin getAdmin() { return mAdmin; }

  /**
   * Get the name of this table.
   *
   * @return the table name.
   */
  public String getTableName() { return mTableName; }

  /** {@inheritDoc} */
  @Override
  public String toString() { return mTableName; }

  // TODO: This might need to do some cleanup, handle some reference counting, etc.
  /**
   * Close the table.
   */
  public void close() { }

  /**
   * Create a new Cassandra table interface for the session / keyspace in this admin and a table
   * name.
   *
   * @param admin Contains pointer to open session for this keyspace.
   * @param tableName Name of the table in question.  Should already exist.
   * @return A new CassandraTableInterface object.
   */
  static CassandraTableInterface createFromCassandraAdmin(CassandraAdmin admin, String tableName) {
    // TODO: Verify that the table already exists!
    return new CassandraTableInterface(admin, tableName);
  }

  /**
   * Create a Cassandra table interface.
   *
   * @param admin The cassandra admin for this table (and Kiji instance).
   * @param tableName The name of the table.
   */
  private CassandraTableInterface(CassandraAdmin admin, String tableName) {
    this.mAdmin = admin;
    this.mTableName = tableName;
  }
}
