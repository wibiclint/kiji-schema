package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.Session;

/**
 * Lightweight wrapper to mimic the functionality of HBaseAdmin.
 *
 * We can pass instances of this class around the C* code instead of passing around just Sessions or something like that.
 *
 * NOTE: We assume that this session does NOT currently have a keyspace selected.
 *
 * TODO: Need to figure out who is in charge of closing out the open session here...
 *
 */
public class CassandraAdmin {

  /** Current C* session for the given keyspace. */
  private final Session mSession;

  public Session getSession() { return mSession; }

  /**
   * Create new instance of CassandraAdmin from an already-open C* session.
   *
   * @return a new CassandraAdmin instance.
   */
  public static CassandraAdmin makeFromOpenSession(Session session, String keyspace) {
    return new CassandraAdmin(session, keyspace);
  }

  /**
   * Create a table in the given keyspace.
   *
   * This wrapper exists so that we can add lots of extra boilerplate checks in here.
   *
   * @param tableName The name of the table to create.
   * @param tableDescription A string with the table layout.
   */
  public void createTable(String tableName, String tableDescription) {
    mSession.execute("CREATE TABLE " + tableName + " " + tableDescription + ";");
  }

  public CassandraTableInterface getCassandraTableInterface(String tableName) {
    // TODO: Some code to make sure that this table actually exists!
    return CassandraTableInterface.createFromCassandraAdmin(this, tableName);
  }

  /**
   * Constructor for creating a C* admin from an open C* session.
   * @param session An open Session connected to a cluster with a keyspace selected.
   * @param keyspace The name of the keyspace to use.
   */
  private CassandraAdmin(Session session, String keyspace) {
    this.mSession = session;
    // TODO: Lots of checks that this keyspace exists, that this command succeeded, etc.
    session.execute("USE " + keyspace);
  }
}
