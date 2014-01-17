package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.ResultSet;
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
public abstract class CassandraAdmin implements Closeable {

  /** Current C* session for the given keyspace. */
  private final Session mSession;

  /** URI for this instance. **/
  private final KijiURI mKijiURI;

  public Session getSession() { return mSession; }

  public String getKeyspace() {
    return KijiManagedCassandraTableName.getCassandraKeyspaceForKijiInstance(mKijiURI.getInstance().toString());
  }

  protected CassandraAdmin(Session session, KijiURI kijiURI) {
    this.mSession = session;
    this.mKijiURI = kijiURI;
    // TODO: We might want to replace this call in the constructor with a public version of the
    // createKeyspaceIfMissingForURI that the installers have to call themselves.
    createKeyspaceIfMissingForURI(mKijiURI);
  }

  /**
   * Given a URI, create a keyspace for the Kiji instance if none yet exists.
   *
   * @param kijiURI The URI.
   */
  private void createKeyspaceIfMissingForURI(KijiURI kijiURI) {
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceForKijiInstance(
        kijiURI.getInstance().toString()
    );

    // TODO: Should check whether the keyspace is longer than 48 characters long and if so provide a Kiji error to the user.
    String queryText = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
        " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}";
    getSession().execute(queryText);
    getSession().execute(String.format("USE %s", keyspace));

  }

  /**
   * Create a table in the given keyspace.
   *
   * This wrapper exists so that we can add lots of extra boilerplate checks in here.
   *
   * @param tableName The name of the table to create.
   * @param cassandraTableLayout A string with the table layout.
   */
  public CassandraTableInterface createTable(String tableName, String cassandraTableLayout) {
    // TODO: Keep track of all tables associated with this session
    mSession.execute("CREATE TABLE " + tableName + " " + cassandraTableLayout + ";");
    return CassandraTableInterface.createFromCassandraAdmin(this, tableName);
  }

  public CassandraTableInterface getCassandraTableInterface(String tableName) {
    // TODO: Some code to make sure that this table actually exists!
    return CassandraTableInterface.createFromCassandraAdmin(this, tableName);
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
