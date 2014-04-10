package org.kiji.schema.impl.cassandra;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;

import org.kiji.schema.CassandraKijiURI;
import org.kiji.schema.KijiURI;

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
   * Create a CassandraAdmin object from a URI.
   *
   * @param kijiURI The KijiURI specifying the Kiji instance for the CassandraAdmin.
   *                Note: Must be an instance of CassandraKijiURI.
   * @return A CassandraAdmin for the given Kiji instance.
   */
  public static DefaultCassandraAdmin makeFromKijiURI(KijiURI kijiURI) {
    Preconditions.checkArgument(kijiURI.isCassandra());
    CassandraKijiURI cassandraKijiURI = (CassandraKijiURI) kijiURI;
    List<String> hosts = cassandraKijiURI.getCassandraNodes();
    String[] hostStrings = hosts.toArray(new String[0]);
    int port = cassandraKijiURI.getCassandraClientPort();
    Cluster cluster = Cluster
        .builder()
        .addContactPoints(hostStrings)
        .withPort(port)
        .build();
    Session cassandraSession = cluster.connect();
    return new DefaultCassandraAdmin(cassandraSession, kijiURI);
  }


  /**
   * Create a CassandraAdmin from an open Cassandra Session and a URI.
   *
   * @param session The open C* Session.
   * @param kijiURI The KijiURI specifying the Kiji instance for the CassandraAdmin.
   *                Note: Must be an instance of CassandraKijiURI.
   */
  private DefaultCassandraAdmin(Session session, KijiURI kijiURI) {
    super(session, kijiURI);
  }
}
