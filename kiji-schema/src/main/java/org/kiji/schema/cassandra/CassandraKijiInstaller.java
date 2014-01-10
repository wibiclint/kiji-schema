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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.kiji.schema.impl.cassandra.CassandraAdmin;
import org.kiji.schema.impl.cassandra.CassandraSystemTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiNotInstalledException;
import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
//import org.kiji.schema.impl.cassandra.CassandraMetaTable;
//import org.kiji.schema.impl.cassandra.CassandraSchemaTable;
//import org.kiji.schema.impl.cassandra.CassandraSystemTable;
import org.kiji.schema.security.KijiSecurityManager;
import org.kiji.schema.util.ResourceUtils;

/** Installs or uninstalls Kiji instances from an Cassandra cluster. */
@ApiAudience.Public
@ApiStability.Evolving
public final class CassandraKijiInstaller {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiInstaller.class);

  /** Singleton CassandraKijiInstaller. **/
  private static final CassandraKijiInstaller SINGLETON = new CassandraKijiInstaller();

  /** Constructs a CassandraKijiInstaller. */
  private CassandraKijiInstaller() {
  }

  /**
   * Installs the specified Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param conf Hadoop configuration (TODO: Not clear if we really need this here).
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  public void install(KijiURI uri, Configuration conf) throws IOException {
    install(uri, Collections.<String, String>emptyMap(), conf);
  }

  /**
   * Installs a Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param properties Map of the initial system properties for installation, to be used in addition
   *     to the defaults.
   * @param conf Hadoop configuration (TODO: Not clear if we really need this here).
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   */
  public void install(
      KijiURI uri,
      Map<String, String> properties,
      Configuration conf)
      throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }

    // Try to create the keyspace.  If it already exists, the then the query will return an
    // AlreadyExistsException.
    // TODO: Figure out a cleaner way to check whether the keyspace already exists.

    try {
      // Create the keyspace for this kiji instance.
      LOG.info(String.format("Installing Cassandra Kiji instance '%s'.", uri));

      // TODO: Replace "localhost" with host from KijiURI.
      Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
      Session cassandraSession = cluster.connect();

      // TODO: Need a way for users to specify strategy, replication, etc.
      // TODO: This "kiji_" should really be added somewhere in KijiManagedCassandraTableName.
      String queryText = "CREATE KEYSPACE kiji_" + uri.getInstance() +
          " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1}";
      ResultSet results = cassandraSession.execute(queryText);

      // Create a CassandraAdmin wrapping around the session to pass to the installation code for system, meta, and schema tables.
      CassandraAdmin cassandraAdmin = CassandraAdmin.makeFromOpenSession(cassandraSession, "kiji_" + uri.getInstance());

      // Install the system, meta, and schema tables.
      CassandraSystemTable.install(cassandraAdmin, uri, conf, properties);
      //HBaseMetaTable.install(hbaseAdmin, uri);
      //HBaseSchemaTable.install(hbaseAdmin, uri, conf, tableFactory, lockFactory);


    } catch (AlreadyExistsException aee) {
      throw new KijiAlreadyExistsException(String.format(
          "Cassandra Kiji instance '%s' already exists.", uri), uri);
    }
    // TODO: Security stuff

    LOG.info(String.format("Installed Cassandra Kiji instance '%s'.", uri));
  }

  /**
   * Removes a kiji instance from the Cassandra cluster including any user tables.
   *
   * @param uri URI of the Kiji instance to install.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid.
   * @throws KijiNotInstalledException if the specified instance does not exist.
   */
  public void uninstall(KijiURI uri, Configuration conf)
      throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }

    try {
      // Just drop the keyspace...
      LOG.info(String.format("Removing the Cassandra Kiji instance '%s'.", uri.getInstance()));

      // TODO: Replace "localhost" with host from KijiURI.
      Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
      Session cassandraSession = cluster.connect();

      // TODO: This "kiji_" should really be added somewhere in KijiManagedCassandraTableName.
      String queryText = "DROP KEYSPACE kiji_" + uri.getInstance();
      ResultSet results = cassandraSession.execute(queryText);
    } catch (QueryExecutionException qee) {
      // TODO: Handle this!
    }

    LOG.info(String.format("Removed Cassandra Kiji instance '%s'.", uri.getInstance()));
  }

  /**
   * Gets an instance of a CassandraKijiInstaller.
   *
   * @return An instance of a CassandraKijiInstaller.
   */
  public static CassandraKijiInstaller get() {
    return SINGLETON;
  }
}
