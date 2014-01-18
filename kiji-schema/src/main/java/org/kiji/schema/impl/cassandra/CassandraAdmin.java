package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;

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

  private static final Logger LOG = LoggerFactory.getLogger(CassandraAdmin.class);

  /** Current C* session for the given keyspace. */
  private final Session mSession;

  /** URI for this instance. **/
  private final KijiURI mKijiURI;

  public Session getSession() { return mSession; }

  public String getKeyspace() {
    return KijiManagedCassandraTableName.getCassandraKeyspaceForKijiInstance(mKijiURI.getInstance().toString());
  }

  protected CassandraAdmin(Session session, KijiURI kijiURI) {
    Preconditions.checkNotNull(session);
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
    LOG.info("Creating table " + tableName);
    mSession.execute("CREATE TABLE " + tableName + " " + cassandraTableLayout + ";");
    return CassandraTableInterface.createFromCassandraAdmin(this, tableName);
  }

  public CassandraTableInterface getCassandraTableInterface(String tableName) {
    // TODO: Some code to make sure that this table actually exists!
    return CassandraTableInterface.createFromCassandraAdmin(this, tableName);
  }

  // TODO: Add something for closing the session and all of the tables.
  public void disableTable(String tableName) { }

  // TODO: Maybe have more checks here?
  public void deleteTable(String tableName) {
    String queryString = String.format("DROP TABLE %s;", tableName);
    LOG.info("Deleting table " + tableName);
    getSession().execute(queryString);
  }

  public void deleteKeyspace() {
    String ks = getKeyspace();

    // Verify that there aren't any tables left in the keyspace.
    Collection<TableMetadata> tableMetadata = getSession().getCluster().getMetadata().getKeyspace(ks).getTables();

    if (tableMetadata.size() != 0) {
      throw new KijiIOException(String.format(
          "Cannot delete keyspace for Kiji instance %s while there are still table present! (tables=%s)",
          mKijiURI.getInstance(),
          tableMetadata.toString()
      ));
    }

    String queryString = String.format(
        "DROP KEYSPACE %s;",
        getKeyspace()
    );
    getSession().execute(queryString);

    Collection<KeyspaceMetadata> keyspaceMetadata = getSession().getCluster().getMetadata().getKeyspaces();
    for (KeyspaceMetadata ksm : keyspaceMetadata) {
      if (ksm.getName() == ks) {
        throw new KijiIOException(String.format("Keyspace delete for %s failed!", ks));
      }
    }
  }

  // TODO: Implement check for whether a table exists!
  public boolean tableExists(String tableName) {
    LOG.debug("Looking for table with name " + tableName);
    Preconditions.checkNotNull(getSession());
    String ks = getKeyspace();
    LOG.debug("\tkeyspace = " + ks);
    Metadata metadata = getSession().getCluster().getMetadata();
    if (null == metadata.getKeyspace(ks)) {
      LOG.debug("\tCannot find keyspace " + ks + ", assuming table " + tableName + " is not installed");
      return false;
    }
    Collection<TableMetadata> tableMetadata = getSession().getCluster().getMetadata().getKeyspace(ks).getTables();
    for (TableMetadata tm : tableMetadata) {
      final String nameWithKeyspace = String.format("%s.%s", ks, tm.getName());
      LOG.debug("\t" + nameWithKeyspace);
      if (nameWithKeyspace.equals(tableName)) {
        return true;
      }
    }
    LOG.debug("\tCould not find any table with name matching table " + tableName);
    return false;
  }

  // TODO: Implement close method
  public void close() {
  }
}
