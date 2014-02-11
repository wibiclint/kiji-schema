/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.cassandra;

import com.datastax.driver.core.Cluster;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.KeeperException;
import org.kiji.delegation.Priority;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.impl.cassandra.CassandraAdminFactory;
import org.kiji.schema.impl.cassandra.DefaultCassandraFactory;
import org.kiji.schema.impl.cassandra.TestingCassandraAdminFactory;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.util.LocalLockFactory;
import org.kiji.schema.util.LockFactory;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/** Factory for Cassandra instances based on URIs. */
public final class TestingCassandraFactory implements CassandraFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestingCassandraFactory.class);

  /** Factory to delegate to. */
  private static final CassandraFactory DELEGATE = new DefaultCassandraFactory();

  /** Lock object to protect MINI_ZOOKEEPER_CLUSTER and MINIZK_CLIENT. */
  private static final Object MINIZK_CLUSTER_LOCK = new Object();

  /** ZooKeeper session timeout, in milliseconds. */
  private static final int ZKCLIENT_SESSION_TIMEOUT = 60 * 1000;  // 1 second

  /**
   * Singleton MiniZooKeeperCluster for testing.
   *
   * Lazily instantiated when the first test requests a ZooKeeperClient for a .fake Kiji instance.
   *
   * Once started, the mini-cluster remains alive until the JVM shuts down.
   */
  private static MiniZooKeeperCluster mMiniZkCluster;

  /**
   * Singleton Cassandra session for testing.
   *
   * Lazily instantiated when the first test requests a C* session for a .fake Kiji instance.
   *
   * Once started, will remain alive until the JVM shuts down.
   */
  private static Session mCassandraSession = null;

  /**
   * ZooKeeperClient used to create chroot directories prior to instantiating test ZooKeeperClients.
   * Lazily instantiated when the first test requests a ZooKeeperClient for a .fake Kiji instance.
   *
   * This client will not be closed properly until the JVM shuts down.
   */
  private static ZooKeeperClient mMiniZkClient;

  /** Map from fake HBase ID to fake (local) lock factories. */
  private final Map<String, LockFactory> mLock = Maps.newHashMap();

  /**
   * Map from fake HBase ID to ZooKeeperClient.
   *
   * <p>
   *   ZooKeeperClient instances in this map are never released:
   *   unless client code releases ZooKeeperClient instance more than once,
   *   the retain counter is always &ge; 1.
   * </p>
   */
  private final Map<String, ZooKeeperClient> mZKClients = Maps.newHashMap();

  /**
   * Public constructor. This should not be directly invoked by users; you should
   * use CassandraFactory.get(), which retains a singleton instance.
   *
   * This constructor needs to be public because the Java service loader must be able to instantiate it.
   */
  public TestingCassandraFactory() {
  }

  /** URIs for fake HBase instances are "kiji://.fake.[fake-id]/instance/table". */
  private static final String FAKE_CASSANDRA_ID_PREFIX = ".fake.";

  /** Resets the testing HBase factory. */
  public void reset() {
    mLock.clear();
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Higher priority than default factory.
    return Priority.HIGH;
  }

  //------------------------------------------------------------------------------------------------
  // URI stuff

  /** {@inheritDoc} */
  @Override
  public CassandraAdminFactory getCassandraAdminFactory(KijiURI uri) {
    if (isFakeCassandraURI(uri)) {
      String fakeCassandraID = getFakeCassandraID(uri);
      Preconditions.checkNotNull(fakeCassandraID);

      // Make sure that the EmbeddedCassandraService is started
      try {
        startEmbeddedCassandraServiceIfNotRunningAndOpenSession();
      } catch (Exception e) {
        throw new KijiIOException("Problem with embedded Cassandra session!" + e);
      }

      // Get an admin factory that will work with the embedded service
      return createFakeCassandraAdminFactory(fakeCassandraID);
    } else {
      return DELEGATE.getCassandraAdminFactory(uri);
    }
  }

  /**
   * Extracts the ID of the fake C* from a Kiji URI.
   *
   * @param uri URI to extract a fake C* ID from.
   * @return the fake C* ID, if any, or null.
   */
  private static String getFakeCassandraID(KijiURI uri) {
    if (uri.getZookeeperQuorum().size() != 1) {
      return null;
    }
    final String zkHost = uri.getZookeeperQuorum().get(0);
    if (!zkHost.startsWith(FAKE_CASSANDRA_ID_PREFIX)) {
      return null;
    }
    assert(isFakeCassandraURI(uri));
    return zkHost.substring(FAKE_CASSANDRA_ID_PREFIX.length());
  }

  /**
   * Check whether this is the URI for a fake Cassandra instance.
   *
   * @param uri The URI in question.
   * @return Whether the URI is for a fake instance or not.
   */
  private static boolean isFakeCassandraURI(KijiURI uri) {
    if (uri.getZookeeperQuorum().size() != 1) {
      return false;
    }
    final String zkHost = uri.getZookeeperQuorum().get(0);
    if (!zkHost.startsWith(FAKE_CASSANDRA_ID_PREFIX)) {
      return false;
    }
    return true;
  }

  //------------------------------------------------------------------------------------------------
  // Stuff for starting up C*

  /**
   * Return a fake C* admin factory for testing.
   * @param fakeCassandraID
   * @return A C* admin factory that will produce C* admins that will all use the shared EmbeddedCassandraService.
   */
  private CassandraAdminFactory createFakeCassandraAdminFactory(String fakeCassandraID) {
    Preconditions.checkNotNull(mCassandraSession);
    return TestingCassandraAdminFactory.get(mCassandraSession);
  }

  /**
   * Ensure that the EmbeddedCassandraService for unit tests is running.  If it is not, then start it.
   */
  private void startEmbeddedCassandraServiceIfNotRunningAndOpenSession() throws Exception {
    LOG.debug("Ready to start a C* service if necessary...");
    if (null != mCassandraSession) {
      LOG.debug("C* is already running, no need to start the service.");
      //Preconditions.checkNotNull(mCassandraSession);
      return;
    }

    LOG.debug("Starting EmbeddedCassandra!");
    try {
      LOG.info("Starting EmbeddedCassandraService...");
      // Use a custom YAML file that specifies different ports from normal for RPC and thrift.
      File yamlFile = new File(getClass().getResource("/cassandra.yaml").getFile());

      assert (yamlFile.exists());
      System.setProperty("cassandra.config", "file:" + yamlFile.getAbsolutePath());
      System.setProperty("cassandra-foreground", "true");

      // Make sure that all of the directories for the commit log, data, and caches are empty.
      // Thank goodness there are methods to get this information (versus parsing the YAML directly).
      ArrayList<String> directoriesToDelete = new ArrayList<String>(Arrays.asList(
          DatabaseDescriptor.getAllDataFileLocations()
      ));
      directoriesToDelete.add(DatabaseDescriptor.getCommitLogLocation());
      directoriesToDelete.add(DatabaseDescriptor.getSavedCachesLocation());
      for (String dirName : directoriesToDelete) {
        FileUtils.deleteDirectory(new File(dirName));
      }
      EmbeddedCassandraService embeddedCassandraService = new EmbeddedCassandraService();
      embeddedCassandraService.start();

    } catch (IOException ioe) {
      throw new KijiIOException("Cannot start embedded C* service!");
    }

    // Use different port from normal here to avoid conflicts with any locally-running C* cluster.
    // Port settings are controlled in "cassandra.yaml" in test resources.
    String hostIp = "127.0.0.1";
    int port = 9043;
    Cluster cluster = Cluster.builder().addContactPoints(hostIp).withPort(port).build();
    mCassandraSession = cluster.connect();
  }

  //------------------------------------------------------------------------------------------------
  // Locks and ZooKeeper stuff

  /** {@inheritDoc} */
  @Override
  public LockFactory getLockFactory(KijiURI uri, Configuration conf) throws IOException {
    final String fakeID = getFakeCassandraID(uri);
    if (fakeID != null) {
      synchronized (mLock) {
        final LockFactory factory = mLock.get(fakeID);
        if (factory != null) {
          return factory;
        }
        final LockFactory newFactory = new LocalLockFactory();
        mLock.put(fakeID, newFactory);
        return newFactory;
      }
    }
    return DELEGATE.getLockFactory(uri, conf);
  }

  /**
   * Creates a new ZooKeeperClient with a chroot set to the fakeId for a KijiClientTest.
   *
   * <p> The client will connect to the testing MiniZooKeeperCluster. </p>
   *
   * @param fakeId the id of the test instance.
   * @return a new ZooKeeperClient for the test instance.
   * @throws java.io.IOException on I/O error.
   */
  private static ZooKeeperClient createZooKeeperClientForFakeId(String fakeId)
      throws IOException {

    // Initializes the Mini ZooKeeperCluster, if necessary:
    final MiniZooKeeperCluster zkCluster = getMiniZKCluster();

    // Create the chroot node for this fake ID, if necessary:
    try {
      final File zkChroot = new File("/" + fakeId);
      if (mMiniZkClient.exists(zkChroot) == null) {
        mMiniZkClient.createNodeRecursively(new File("/" + fakeId));
      }
    } catch (KeeperException ke) {
      throw new KijiIOException(ke);
    }

    // Test ZooKeeperClients use a chroot to isolate testing environments.
    final String zkAddress = "localhost:" + zkCluster.getClientPort() + "/" + fakeId;

    Log.info("Creating test ZooKeeperClient for address {}", zkAddress);
    final ZooKeeperClient zkClient = new ZooKeeperClient(zkAddress, ZKCLIENT_SESSION_TIMEOUT);
    zkClient.open();
    return zkClient;
  }

  /**
   * Creates a new MiniZooKeeperCluster if one does not already exist.  Also creates and opens a
   * ZooKeeperClient for the minicluster which is used to create chroot nodes before opening test
   * ZooKeeperClients.
   *
   * @throws java.io.IOException in case of an error creating the temporary directory or starting the mini
   *    zookeeper cluster.
   */
  private static MiniZooKeeperCluster getMiniZKCluster() throws IOException {
    synchronized (MINIZK_CLUSTER_LOCK) {
      if (mMiniZkCluster == null) {
        final MiniZooKeeperCluster miniZK = new MiniZooKeeperCluster(new Configuration());
        final File tempDir = File.createTempFile("mini-zk-cluster", "dir");
        Preconditions.checkState(tempDir.delete());
        Preconditions.checkState(tempDir.mkdirs());
        try {
          miniZK.startup(tempDir);
        } catch (InterruptedException ie) {
          throw new RuntimeInterruptedException(ie);
        }
        mMiniZkCluster = miniZK;

        final String zkAddress ="localhost:" + mMiniZkCluster.getClientPort();
        LOG.info("Creating testing utility ZooKeeperClient for {}", zkAddress);
        mMiniZkClient = new ZooKeeperClient(zkAddress, ZKCLIENT_SESSION_TIMEOUT);
        mMiniZkClient.open();
      }
      return mMiniZkCluster;
    }
  }

  /**
   * Returns the ZooKeeperClient to use for the instance with the specified fake ID.
   *
   * @param fakeId ID of the fake testing instance to get a ZooKeeperClient for.
   * @return the ZooKeeperClient for the instance with the specified fake ID.
   * @throws java.io.IOException on I/O error.
   */
  private ZooKeeperClient getTestZooKeeperClient(String fakeId) throws IOException {
    synchronized (mZKClients) {
      ZooKeeperClient zkClient = mZKClients.get(fakeId);
      if (null == zkClient) {
        zkClient = createZooKeeperClientForFakeId(fakeId);
        mZKClients.put(fakeId, zkClient);
      }
      return zkClient.retain();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   *   TestingCassandraFactory manages a pool of connections to a singleton MiniZooKeeperCluster for
   *   unit testing purposes.  These connections are lazily created and reused where possible.
   * </p>
   */
  @Override
  public ZooKeeperClient getZooKeeperClient(KijiURI uri) throws IOException {
    final String fakeId = getFakeCassandraID(uri);
    if (fakeId != null) {
      return getTestZooKeeperClient(fakeId);
    }

    // Not a test instance, delegate to default factory:
    return DELEGATE.getZooKeeperClient(uri);
  }
}
