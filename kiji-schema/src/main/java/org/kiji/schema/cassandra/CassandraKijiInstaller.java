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

import com.datastax.driver.core.exceptions.AlreadyExistsException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAlreadyExistsException;
import org.kiji.schema.KijiInvalidNameException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.cassandra.CassandraAdmin;
import org.kiji.schema.impl.cassandra.CassandraAdminFactory;
import org.kiji.schema.impl.cassandra.CassandraKijiFactory;
import org.kiji.schema.impl.cassandra.CassandraMetaTable;
import org.kiji.schema.impl.cassandra.CassandraSchemaTable;
import org.kiji.schema.impl.cassandra.CassandraSystemTable;
import org.kiji.schema.security.CassandraKijiSecurityManager;
import org.kiji.schema.util.LockFactory;
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
   * @param conf Hadoop configuration
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the Kiji instance name is invalid or already exists.
   */
  public void install(KijiURI uri, Configuration conf) throws IOException {
    install(uri, CassandraFactory.Provider.get(), Collections.<String, String>emptyMap(), conf);
  }

  /**
   * Installs a Kiji instance.
   *
   * @param uri URI of the Kiji instance to install.
   * @param cassandraFactory C* factory.
   * @param properties Map of the initial system properties for installation, to be used in addition
   *     to the defaults.
   * @param conf Hadoop configuration
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid or already exists.
   */
  public void install(
      KijiURI uri,
      CassandraFactory cassandraFactory,
      Map<String, String> properties,
      Configuration conf)
      throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }

    if (!uri.isCassandra()) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' is not a valid Cassandra-Kiji URI", uri));
    }

    final LockFactory lockFactory = cassandraFactory.getLockFactory(uri, conf);

    try {
      LOG.info(String.format("Installing Cassandra Kiji instance '%s'.", uri));

      CassandraAdminFactory cassandraAdminFactory = cassandraFactory.getCassandraAdminFactory(uri);
      LOG.info("Creating CassandraAdmin for Kiji installation.");
      CassandraAdmin cassandraAdmin = cassandraAdminFactory.create(uri);

      // Install the system, meta, and schema tables.
      CassandraSystemTable.install(cassandraAdmin, uri, conf, properties);
      CassandraMetaTable.install(cassandraAdmin, uri);
      CassandraSchemaTable.install(cassandraAdmin, uri, conf, lockFactory);

      // Grant the current user all privileges on the instance just created, if security is enabled.
      //final Kiji kiji = CassandraKijiFactory.get().open(uri, conf, cassandraAdmin, lockFactory);
      final Kiji kiji = CassandraKijiFactory.get().open(uri, conf);
      try {
        if (kiji.isSecurityEnabled()) {
          CassandraKijiSecurityManager.installInstanceCreator(uri, conf, cassandraAdmin);
        }
      } finally {
        kiji.release();
      }

    } catch (AlreadyExistsException aee) {
      throw new KijiAlreadyExistsException(String.format(
          "Cassandra Kiji instance '%s' already exists.", uri), uri);
    }
    // TODO (SCHEMA-706): Add security checks when we have a plan for security in Cassandra Kiji.

    LOG.info(String.format("Installed Cassandra Kiji instance '%s'.", uri));
  }

  /**
   * Removes a kiji instance from the Cassandra cluster including any user tables.
   *
   * @param uri URI of the Kiji instance to install.
   * @param conf Hadoop configuration.
   * @throws IOException on I/O error.
   * @throws KijiInvalidNameException if the instance name is invalid.
   */
  public void uninstall(KijiURI uri, Configuration conf)
      throws IOException {
    if (uri.getInstance() == null) {
      throw new KijiInvalidNameException(String.format(
          "Kiji URI '%s' does not specify a Kiji instance name", uri));
    }
    final CassandraAdminFactory adminFactory =
        CassandraFactory.Provider.get().getCassandraAdminFactory(uri);

    LOG.info(String.format("Removing the Cassandra Kiji instance '%s'.", uri.getInstance()));

    final Kiji kiji = CassandraKijiFactory.get().open(uri, conf);
    try {
      // TODO (SCHEMA-706): Add security checks when we have a plan for security in Cassandra Kiji.

      for (String tableName : kiji.getTableNames()) {
        LOG.debug("Deleting kiji table " + tableName + "...");
        kiji.deleteTable(tableName);
      }
      // Delete the user tables:
      final CassandraAdmin admin = adminFactory.create(uri);
      try {

        // Delete the system tables:
        CassandraSystemTable.uninstall(admin, uri);
        CassandraMetaTable.uninstall(admin, uri);
        CassandraSchemaTable.uninstall(admin, uri);

      } finally {
        ResourceUtils.closeOrLog(admin);
      }

      // Assert that there are no tables left and delete the keyspace.
      assert(admin.keyspaceIsEmpty());
      admin.deleteKeyspace();

    } finally {
      kiji.release();
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
