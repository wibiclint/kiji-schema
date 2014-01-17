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
public interface CassandraAdmin extends Closeable {

  public Session getSession();

  /**
   * Create a table in the given keyspace.
   *
   * This wrapper exists so that we can add lots of extra boilerplate checks in here.
   *
   * @param tableName The name of the table to create.
   * @param tableDescription A string with the table layout.
   */
  public CassandraTableInterface createTable(String tableName, String tableDescription);

  public CassandraTableInterface getCassandraTableInterface(String tableName);

  // TODO: Add something for closing the session and all of the tables.
  public void disableTable(String tableName);

  public void deleteTable(String tableName);

  // TODO: Implement check for whether a table exists!
  public boolean tableExists(String tableName);

  // TODO: Implement close method
  public void close();

  /**
   * Inner class with shared code for different instantiations of CassandraAdmin.
   *
   */
  static class CassandraAdminUtil {


  }
}
