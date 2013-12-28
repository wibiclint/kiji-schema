package org.kiji.schema.impl.cassandra;
import com.datastax.driver.core.Session;

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
public class CassandraTableInterface {

  /** C* Admin with open session. */
  private final CassandraAdmin mAdmin;

  /** Name of the table. */
  private final String mTableName;

  public CassandraAdmin getAdmin() { return mAdmin; }

  public String getTableName() { return mTableName; }

  public String toString() { return mTableName; }

  /**
   * TODO: This might need to do some cleanup, handle some reference counting, etc.
   */
  public void close() { }

  /**
   * Create a new Cassandra table interface for the session / keyspace in this admin and a table name.
   *
   * @param admin Contains pointer to open session for this keyspace.
   * @param tableName Name of the table in question.  Should already exist.
   * @return A new CassandraTableInterface object.
   */
  static CassandraTableInterface createFromCassandraAdmin(CassandraAdmin admin, String tableName) {
    // TODO: Verify that the table already exists!
    return new CassandraTableInterface(admin, tableName);
  }

  private CassandraTableInterface(CassandraAdmin admin, String tableName) {
    this.mAdmin = admin;
    this.mTableName = tableName;
  }
}
