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
public class TestingCassandraAdmin extends CassandraAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(TestingCassandraAdmin.class);

  /**
   * Constructor for creating a C* admin from an open C* session.
   * @param session An open Session connected to a cluster with a keyspace selected.
   * @param keyspace The name of the keyspace to use.
   */
  private TestingCassandraAdmin(Session session, KijiURI kijiURI) {
    super(session, kijiURI);
  }

  /**
   * Create new instance of CassandraAdmin from an already-open C* session.
   *
   * @param session An already-created Cassandra session for testing.
   * @param kijiURI The URI of the Kiji instance to create.
   * @return A C* admin instance for the Kiji instance.
   */
  public static TestingCassandraAdmin makeFromKijiURI(Session session, KijiURI kijiURI) {
    return new TestingCassandraAdmin(session, kijiURI);
  }
}
