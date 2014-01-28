package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Preconditions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.KijiManagedCassandraTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
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

  private static String stripQuotesFromKeyspace(String keyspace) {
    return keyspace.substring(1, keyspace.length()-1);
  }

  private static String stripQuotesFromTableName(String tableName) {
    // If the table name looks like keyspace.table, then we have to strip off both!  Yikes!
    // This would be one trivial line of Python...
    return tableName.replace("\"", "");
  }

  /** Current C* session for the given keyspace. */
  private final Session mSession;

  /** URI for this instance. **/
  private final KijiURI mKijiURI;

  public Session getSession() { return mSession; }

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
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceFormattedForCQL(kijiURI);
    LOG.info(String.format("Creating keyspace %s (if missing) for %s.", keyspace, kijiURI));

    // TODO: Should check whether the keyspace is longer than 48 characters long and if so provide a Kiji error to the user.
    String queryText = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
        " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}";
    ResultSet resultSet = getSession().execute(queryText);
    LOG.info(resultSet.toString());
    getSession().execute(String.format("USE %s", keyspace));

    // Check that the keyspace actually exists!
    assert(keyspaceExists(keyspace));

  }

  private boolean keyspaceExists(String keyspace) {
    Preconditions.checkArgument(KijiManagedCassandraTableName.keyspaceNameIsFormattedForCQL(keyspace));

    // Strip quotes off of keyspace
    String noQuotesKeyspace = stripQuotesFromKeyspace(keyspace);
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
   * This wrapper exists so that we can add lots of extra boilerplate checks in here.
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
    String keyspace = KijiManagedCassandraTableName.getCassandraKeyspaceFormattedForCQL(mKijiURI);
    Collection<TableMetadata> tables =
        getSession().getCluster().getMetadata().getKeyspace(keyspace).getTables();
    return (tables.isEmpty());
  }

  public void deleteKeyspace() {
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
    String keyspace = stripQuotesFromKeyspace(
        KijiManagedCassandraTableName.getCassandraKeyspaceFormattedForCQL(mKijiURI)
    );

    String tableNameNoQuotes = stripQuotesFromTableName(tableName);

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
  }
}
