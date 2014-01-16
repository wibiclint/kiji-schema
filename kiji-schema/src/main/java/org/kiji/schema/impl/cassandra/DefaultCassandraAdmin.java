package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;

import java.io.Closeable;

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
public class DefaultCassandraAdmin implements CassandraAdmin {

  /** Current C* session for the given keyspace. */
  private final Session mSession;

  public Session getSession() { return mSession; }

  /**
   * Create new instance of CassandraAdmin from an already-open C* session.
   *
   * @return a new CassandraAdmin instance.
   */
  //public static DefaultCassandraAdmin makeFromOpenSession(Session session, String keyspace) {
    //return new DefaultCassandraAdmin(session, keyspace);
  //}

  public static DefaultCassandraAdmin makeFromKijiURI(KijiURI kijiURI) {
    // TODO: Replace "localhost" with host from KijiURI.
    Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
    Session cassandraSession = cluster.connect();
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceForKijiInstance(kijiURI.getInstance());
    return new DefaultCassandraAdmin(cassandraSession, keyspace);
  }

  /**
   * Create a table in the given keyspace.
   *
   * This wrapper exists so that we can add lots of extra boilerplate checks in here.
   *
   * @param tableName The name of the table to create.
   * @param tableDescription A string with the table layout.
   */
  public CassandraTableInterface createTable(String tableName, String tableDescription) {
    // TODO: Keep track of all tables associated with this session
    mSession.execute("CREATE TABLE " + tableName + " " + tableDescription + ";");
    return CassandraTableInterface.createFromCassandraAdmin(this, tableName);
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
  private DefaultCassandraAdmin(Session session, String keyspace) {
    this.mSession = session;
    // TODO: Lots of checks that this keyspace exists, that this command succeeded, etc.
    session.execute("USE " + keyspace);
  }

  // TODO: Add something for closing the session and all of the tables.
  public void disableTable(String tableName) { }

  public void deleteTable(String tableName) { }

  // TODO: Implement check for whether a table exists!
  public boolean tableExists(String tableName) {
    return true;
  }

  // TODO: Implement close method
  public void close() {
  }
}
