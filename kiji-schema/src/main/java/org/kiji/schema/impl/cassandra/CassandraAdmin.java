package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;

/**
 * Lightweight wrapper to mimic the functionality of HBaseAdmin (and provide other functionality).
 *
 * This class exists mostly so that we are not passing around instances of
 * com.datastax.driver.core.Session everywhere.
 *
 * TODO: Handle reference counting, closing of Session.
 *
 */
public abstract class CassandraAdmin implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraAdmin.class);

  /** Current C* session for the Kiji instance.. */
  private final Session mSession;

  /** URI for this instance. **/
  private final KijiURI mKijiURI;

  /**
   * Remove quotes around a nameWithQuotes, e.g., turn
   *     "nameWithQuotes"
   * into
   *     nameWithQuotes.
   * We need quotes for CQL statements, but not if we are doing direct queries with the DataStax
   * Java driver.
   *
   * @param nameWithQuotes The nameWithQuotes with quotes.
   * @return The nameWithQuotes without quotes.
   */
  private static String stripQuotes(String nameWithQuotes) {
    return nameWithQuotes.replace("\"", "");
  }

  /**
   * Getter for open Session.
   *
   * @return The Session.
   */
  public Session getSession() { return mSession; }

  /**
   * Constructor for use by classes that extend this class.  Creates a CassandraAdmin object for a
   * given Kiji instance.
   *
   * @param session Session for this Kiji instance.
   * @param kijiURI URI for this Kiji instance.
   */
  protected CassandraAdmin(Session session, KijiURI kijiURI) {
    Preconditions.checkNotNull(session);
    this.mSession = session;
    this.mKijiURI = kijiURI;
    createKeyspaceIfMissingForURI(mKijiURI);
  }

  /**
   * Given a URI, create a keyspace for the Kiji instance if none yet exists.
   *
   * @param kijiURI The URI.
   */
  private void createKeyspaceIfMissingForURI(KijiURI kijiURI) {
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceFormattedForCQL(kijiURI);
    LOG.info(String.format("Creating keyspace %s (if missing) for %s.", keyspace, kijiURI));

    // TODO: Check whether keyspace is > 48 characters long and if so provide Kiji error to the user.
    String queryText = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
        " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}";
    ResultSet resultSet = getSession().execute(queryText);
    LOG.info(resultSet.toString());
    getSession().execute(String.format("USE %s", keyspace));

    // Check that the keyspace actually exists!
    assert(keyspaceExists(keyspace));

  }

  /**
   * Check whether a keyspace exists.
   *
   * @param keyspace The keyspace name (can include quotes - this method strips them).
   * @return Whether the keyspace exists.
   */
  private boolean keyspaceExists(String keyspace) {
    Preconditions.checkArgument(KijiManagedCassandraTableName.keyspaceNameIsFormattedForCQL(keyspace));

    // Strip quotes off of keyspace
    String noQuotesKeyspace = stripQuotes(keyspace);
    LOG.debug("keyspace without quotes = " + noQuotesKeyspace);

    LOG.debug("Checking whether keyspace " + noQuotesKeyspace + " exists.");
    Metadata md = getSession().getCluster().getMetadata();
    LOG.debug("Found these keyspaces:");
    for (KeyspaceMetadata ksm : md.getKeyspaces()) {
      LOG.debug(String.format("\t%s", ksm.getName()));
    }
    return (null != md.getKeyspace(noQuotesKeyspace));
  }

  /**
   * Create a table in the given keyspace.
   *
   * This wrapper exists (rather than having various classes create tables themselves) so that we
   * can add lots of extra boilerplate checks in here.
   *
   * @param tableName The name of the table to create.
   * @param cassandraTableLayout A string with the table layout.
   */
  public CassandraTableInterface createTable(String tableName, String cassandraTableLayout) {
    Preconditions.checkArgument(KijiManagedCassandraTableName.tableNameIsFormattedForCQL(tableName));

    // TODO: Keep track of all tables associated with this session
    LOG.info("Creating table " + tableName);
    mSession.execute("CREATE TABLE " + tableName + " " + cassandraTableLayout + ";");

    // Check that the table actually exists
    assert(tableExists(tableName));
    return CassandraTableInterface.createFromCassandraAdmin(this, tableName);
  }

  /**
   * Returns an interface to a Cassandra table.
   *
   * The `CassandraTableInterface` object is meant to provide functionality similar to that of
   * `HTableInterface`.
   *
   * @param tableName The name of the table for which to return an interface.
   * @return The interface for the specified Cassandra table.
   */
  public CassandraTableInterface getCassandraTableInterface(String tableName) {
    Preconditions.checkArgument(KijiManagedCassandraTableName.tableNameIsFormattedForCQL(tableName));
    assert(tableExists(tableName));
    return CassandraTableInterface.createFromCassandraAdmin(this, tableName);
  }

  // TODO: Add something for disabling this table.
  public void disableTable(String tableName) { }

  // TODO: Just return true for now since we aren't disabling any Cassandra tables yet.
  public boolean isTableEnabled(String tableName) {
    return true;
  }

  public void deleteTable(String tableName) {
    Preconditions.checkArgument(KijiManagedCassandraTableName.tableNameIsFormattedForCQL(tableName));
    // TODO: Check first that the table actually exists?
    String queryString = String.format("DROP TABLE IF EXISTS %s;", tableName);
    LOG.info("Deleting table " + tableName);
    getSession().execute(queryString);
  }

  public boolean keyspaceIsEmpty() {
    Preconditions.checkNotNull(getSession());
    Preconditions.checkNotNull(getSession().getCluster());
    Preconditions.checkNotNull(getSession().getCluster().getMetadata());
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceFormattedForCQL(mKijiURI);
    String noQuotesKeyspace = stripQuotes(keyspace);
    Preconditions.checkNotNull(getSession().getCluster().getMetadata().getKeyspace(noQuotesKeyspace));
    Collection<TableMetadata> tables =
        getSession().getCluster().getMetadata().getKeyspace(noQuotesKeyspace).getTables();
    return (tables.isEmpty());
  }

  public void deleteKeyspace() {
    // TODO: Track whether keyspace exists and assert appropriate keyspace state in all methods.
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceFormattedForCQL(mKijiURI);
    String queryString = "DROP KEYSPACE " + keyspace;
    getSession().execute(queryString);
    assert (!keyspaceExists(keyspace));
  }

  public boolean tableExists(String tableName) {
    Preconditions.checkArgument(KijiManagedCassandraTableName.tableNameIsFormattedForCQL(tableName));
    Preconditions.checkNotNull(getSession());

    // Remove the quotes from the keyspace and the tablename for comparing against what we find in
    // the C* metadata.
    String keyspace = stripQuotes(
        KijiManagedCassandraTableName.getCassandraKeyspaceFormattedForCQL(mKijiURI)
    );

    String tableNameNoQuotes = stripQuotes(tableName);

    LOG.debug("Looking for table with name " + tableNameNoQuotes);
    LOG.debug("\tkeyspace (w/o quotes) = " + keyspace);

    Metadata metadata = getSession().getCluster().getMetadata();
    if (null == metadata.getKeyspace(keyspace)) {
      LOG.debug("\tCannot find keyspace " + keyspace + ", assuming table " + tableNameNoQuotes + " is not installed");
      return false;
    }
    Collection<TableMetadata> tableMetadata = getSession().getCluster().getMetadata().getKeyspace(keyspace).getTables();
    for (TableMetadata tm : tableMetadata) {
      final String nameWithKeyspace = String.format("%s.%s", keyspace, tm.getName());
      LOG.debug("\t" + nameWithKeyspace);
      if (nameWithKeyspace.equals(tableNameNoQuotes)) {
        return true;
      }
    }
    LOG.debug("\tCould not find any table with name matching table " + tableNameNoQuotes);
    return false;
  }

  // TODO: Implement close method
  public void close() {
    // Cannot close this right now without wreaking havoc in the unit tests.
    //getSession().shutdown();
  }
}
