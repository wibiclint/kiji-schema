package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.Cluster;
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
public class DefaultCassandraAdmin extends CassandraAdmin {

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
    return new DefaultCassandraAdmin(cassandraSession, kijiURI);
  }


  /**
   * Constructor for creating a C* admin from an open C* session.
   * @param session An open Session connected to a cluster with a keyspace selected.
   * @param keyspace The name of the keyspace to use.
   */
  private DefaultCassandraAdmin(Session session, KijiURI kijiURI) {
    super(session, kijiURI);
  }
}
