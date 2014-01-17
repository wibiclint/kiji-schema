package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.*;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;

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
public class TestingCassandraAdmin implements CassandraAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(TestingCassandraAdmin.class);

  /** Current C* session for the given keyspace. */
  private final Session mSession;

  public Session getSession() { return mSession; }

  // TODO: Figure out how to share this code with DefaultCassandraInstance.
  /**
   * Create new instance of CassandraAdmin from an already-open C* session.
   *
   * @param session An already-created Cassandra session for testing.
   * @param kijiURI The URI of the Kiji instance to create.
   * @return A C* admin instance for the Kiji instance.
   */
  public static TestingCassandraAdmin makeFromKijiURI(Session session, KijiURI kijiURI) {
    createKeyspaceIfMissingForURI(session, kijiURI);
    // TODO: Some sanity checks
    LOG.debug("Keyspaces present:");
    List<KeyspaceMetadata> keyspaces = session.getCluster().getMetadata().getKeyspaces();
    for (KeyspaceMetadata ksm : keyspaces) {
      LOG.debug(ksm.getName());
    }
    // TODO: Replace "localhost" with host from KijiURI.
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceForKijiInstance(kijiURI.getInstance());
    return new TestingCassandraAdmin(session, keyspace);
  }

  /**
   * Given a URI, create a keyspace for the Kiji instance if none yet exists.
   *
   * @param kijiURI The URI.
   */
  private static void createKeyspaceIfMissingForURI(Session session, KijiURI kijiURI) {
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceForKijiInstance(
        kijiURI.getInstance().toString()
    );

    // TODO: Should check whether the keyspace is longer than 48 characters long and if so provide a Kiji error to the user.
    String queryText = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
        " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}";
    ResultSet results = session.execute(queryText);
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
  private TestingCassandraAdmin(Session session, String keyspace) {
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
