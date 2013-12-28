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

package org.kiji.schema;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.util.KijiNameValidator;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * URI that uniquely identifies a Cassandra Kiji instance, table, column(s).
 * Currently there is no easy way to select the default URI.
 *
 * <p>
 *   KijiURI objects can be constructed directly from parsing a URI string:
 * </p>
 * <pre><code>
 *   final KijiURI uri = KijiURI.newBuilder("kiji-cassandra://127.0.0.1:2181/127.0.0.1/9160/default/mytable/col").build();
 * </code></pre>
 *
 * <p>
 *   Alternately, CassandraKijiURI objects can be constructed from components by using the builder:
 * </p>
 * <pre><code>
 *   final CassandraKijiURI uri = CassandraKijiURI.newBuilder()
 *     .withZookeeperQuorum("127.0.0.1")
 *     .withZookeeperClientPort(2181)
 *     .withCassandraContactPoint("127.0.0.1")
 *     .withCassandraClientPort(9042)
 *     .withInstanceName("default")
 *     .withTableName("mytable")
 *     .addColumnName(new KijiColumnName(col))
 *     .build();
 * </code></pre>
 *
 * Valid URI forms look like:
 * <li> "kiji://zkHost/cHost/cPort"
 * <li> "kiji://zkHost/(cHost1,cHost2)/cPort"
 * <li> "kiji://zkHost/cHost/cPort/instance"
 * <li> "kiji://zkHost/cHost/cPort/instance/table"
 * <li> "kiji://zkHost:zkPort/cHost/cPort/instance/table"
 * <li> "kiji://zkHost1,zkHost2/cHost/cPort/instance/table"
 * <li> "kiji://(zkHost1,zkHost2):zkPort/cHost/cPort/instance/table"
 * <li> "kiji://zkHost/cHost/cPort/instance/table/col"
 * <li> "kiji://zkHost/cHost/cPort/instance/table/col1,col2"
 */
@ApiAudience.Public
@ApiStability.Stable
public final class CassandraKijiURI extends KijiURI {

  /** URI/URL scheme used to fully qualify a Kiji table. */
  public static final String KIJI_SCHEME = "kiji-cassandra";

  /** Default Cassandra port. */
  public static final int DEFAULT_CASSANDRA_CLIENT_PORT = 9160;

  /** Default Cassandra host. */
  public static final String DEFAULT_CASSANDRA_HOST = "127.0.0.1";

  /**
   * Ordered list of Cassandra cluster host names or IP addresses.
   * Preserves user ordering. Never null.
   */
  private final ImmutableList<String> mCassandraNodes;

  /** Normalized (sorted) version of mZookeeperQuorum. Never null. */
  private final ImmutableList<String> mCassandraNodesNormalized;

  /** Cassandra client port number. */
  private final int mCassandraClientPort;

  /**
   * Constructs a new CassandraKijiURI with the given parameters.
   *
   * @param zookeeperQuorum Zookeeper quorum.
   * @param zookeeperClientPort Zookeeper client port.
   * @param cassandraNodes
   * @param cassandraClientPort
   * @param instanceName Instance name.
   * @param tableName Table name.
   * @param columnNames Column names.
   * @throws org.kiji.schema.KijiURIException If the parameters are invalid.
   */
  private CassandraKijiURI(
      Iterable<String> zookeeperQuorum,
      int zookeeperClientPort,
      Iterable<String> cassandraNodes,
      int cassandraClientPort,
      String instanceName,
      String tableName,
      Iterable<KijiColumnName> columnNames) {
    super(zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
    mCassandraNodes = ImmutableList.copyOf(cassandraNodes);
    mCassandraNodesNormalized = ImmutableSortedSet.copyOf(mCassandraNodes).asList();
    mCassandraClientPort = cassandraClientPort;
  }

  private static Iterable<String> parseZookeeperQuorum(URI uri) {
    final AuthorityParser parser = new AuthorityParser(uri);
    return parser.getZookeeperQuorum();
  }

  private static int parseZookeeperClientPort(URI uri) {
    final AuthorityParser parser = new AuthorityParser(uri);
    return parser.getZookeeperClientPort();
  }

  private static Iterable<String> parseCassandraNodes(URI uri) {
    final String[] pathWithCassandraInfo = new File(uri.getPath()).toString().split("/");

    // Grab the Cassandra hosts and port.
    if (pathWithCassandraInfo.length < 3) {
      throw new KijiURIException(uri.toString(),
          "Invalid path, expecting at least '/cassandra-hosts/cassandra-port'");
    }

    String hosts = pathWithCassandraInfo[1];

    // Check for ( and )
    if (hosts.startsWith("(")) {
      if (!hosts.endsWith(")")) {
        throw new KijiURIException(uri.toString(), "Invalid Cassandra host list");
      }
      hosts = hosts.substring(1, hosts.length()-1);
    }

    String[] hostList = hosts.split(",");
    return new ImmutableList.Builder<String>().addAll(Arrays.asList(hostList)).build();
  }

  private static int parseCassandraClientPort(URI uri) {
    final String[] pathWithCassandraInfo = new File(uri.getPath()).toString().split("/");

    // Grab the Cassandra hosts and port.
    if (pathWithCassandraInfo.length < 3) {
      throw new KijiURIException(uri.toString(),
          "Invalid path, expecting at least '/cassandra-hosts/cassandra-port/'");
    }
    try {
      return Integer.parseInt(pathWithCassandraInfo[2]);
    } catch (NumberFormatException nfe) {
      throw new KijiURIException(uri.toString(),
          "Could not parse Cassandra client port '" + pathWithCassandraInfo[2] + "'");
    }
  }

  private static String parseInstanceName(URI uri) {
    final String[] pathWithCassandraInfo = new File(uri.getPath()).toString().split("/");
    // Copy the Kiji parts of the path
    final String[] path = Arrays.copyOfRange(pathWithCassandraInfo, 3, pathWithCassandraInfo.length);

    if (path.length > 3) {
      throw new KijiURIException(uri.toString(),
          "Invalid path, expecting '/kiji-instance/table-name/(column1, column2, ...)'");
    }
    // Instance name:
    String instanceName;
    if (path.length >= 1) {
      instanceName = (path[0].equals(UNSET_URI_STRING)) ? null: path[0];
    } else {
      instanceName = null;
    }
    return instanceName;
  }

  private static String parseTableName(URI uri) {

    final String[] pathWithCassandraInfo = new File(uri.getPath()).toString().split("/");
    // Copy the Kiji parts of the path
    final String[] path = Arrays.copyOfRange(pathWithCassandraInfo, 3, pathWithCassandraInfo.length);

    if (path.length > 3) {
      throw new KijiURIException(uri.toString(),
          "Invalid path, expecting '/kiji-instance/table-name/(column1, column2, ...)'");
    }

    // Table name:
    String tableName;
    if (path.length >= 2) {
      tableName = (path[1].equals(UNSET_URI_STRING)) ? null : path[1];
    } else {
      tableName = null;
    }
    return tableName;
  }

  private static Iterable<KijiColumnName> parseColumnNames(URI uri) {
    final String[] pathWithCassandraInfo = new File(uri.getPath()).toString().split("/");
    // Copy the Kiji parts of the path
    final String[] path = Arrays.copyOfRange(pathWithCassandraInfo, 3, pathWithCassandraInfo.length);

    if (path.length > 3) {
      throw new KijiURIException(uri.toString(),
          "Invalid path, expecting '/kiji-instance/table-name/(column1, column2, ...)'");
    }
    // Columns:
    final ImmutableList.Builder<KijiColumnName> builder = ImmutableList.builder();
    if (path.length >= 3) {
      if (!path[2].equals(UNSET_URI_STRING)) {
        String[] split = path[2].split(",");
        for (String name : split) {
          builder.add(new KijiColumnName(name));
        }
      }
    }
    return builder.build();
  }



  /**
   * Constructs a URI that fully qualifies a Cassandra-backed Kiji table.
   *
   * @param uri Kiji URI
   * @throws org.kiji.schema.KijiURIException if the URI is invalid.
   */
  private CassandraKijiURI(URI uri) {
    this(
        parseZookeeperQuorum(uri),
        parseZookeeperClientPort(uri),
        parseCassandraNodes(uri),
        parseCassandraClientPort(uri),
        parseInstanceName(uri),
        parseTableName(uri),
        parseColumnNames(uri));
    if (!uri.getScheme().equals(KIJI_SCHEME)) {
      throw new KijiURIException(uri.toString(), "URI scheme must be '" + KIJI_SCHEME + "'");
    }
  }

  /**
   * Builder class for constructing KijiURIs.
   */
  public static final class CassandraKijiURIBuilder extends KijiURI.KijiURIBuilder {
    /**
     * Cassandra nodes: comma-separated list of Cassandra host names or IP addresses.
     * Preserves user ordering.
     */
    private ImmutableList<String> mCassandraNodes;

    /** Cassandra client port number. */
    private int mCassandraClientPort;

    /**
     * Constructs a new builder for KijiURIs.
     *
     * @param zookeeperQuorum The initial zookeeper quorum.
     * @param zookeeperClientPort The initial zookeeper client port.
     * @param instanceName The initial instance name.
     * @param tableName The initial table name.
     * @param columnNames The initial column names.
     */
    private CassandraKijiURIBuilder(
        Iterable<String> zookeeperQuorum,
        int zookeeperClientPort,
        Iterable<String> cassandraNodes,
        int cassandraClientPort,
        String instanceName,
        String tableName,
        Iterable<KijiColumnName> columnNames) {
      super(zookeeperQuorum, zookeeperClientPort, instanceName, tableName, columnNames);
      mCassandraNodes = ImmutableList.copyOf(cassandraNodes);
      mCassandraClientPort = cassandraClientPort;
    }

    /**
     * Constructs a new builder for KijiURIs with default values.
     * See {@link org.kiji.schema.CassandraKijiURI#newBuilder()} for specific values.
     */
    private CassandraKijiURIBuilder() {
      super();
      // Assign defaults for Cassandra nodes, ports
      mCassandraNodes = ImmutableList.copyOf(new String[] {DEFAULT_CASSANDRA_HOST});
      mCassandraClientPort = DEFAULT_CASSANDRA_CLIENT_PORT;

    }

    /**
     * Configures the CassandraKijiURI with Cassandra nodes.
     *
     * @param cassandraNodes The C* nodes.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder withCassandraNodes(String[] cassandraNodes) {
      mCassandraNodes = ImmutableList.copyOf(cassandraNodes);
      return this;
    }

    /**
     * Configures the CassandraKijiURI with the C* client port.
     *
     * @param cassandraClientPort The port.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder withCassandraClientPort(int cassandraClientPort) {
      mCassandraClientPort = cassandraClientPort;
      return this;
    }
    /**
     * Configures the CassandraKijiURI with Zookeeper Quorum.
     *
     * @param zookeeperQuorum The zookeeper quorum.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder withZookeeperQuorum(String[] zookeeperQuorum) {
      return (CassandraKijiURIBuilder) super.withZookeeperQuorum(zookeeperQuorum);
    }

    /**
     * Configures the CassandraKijiURI with the Zookeeper client port.
     *
     * @param zookeeperClientPort The port.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder withZookeeperClientPort(int zookeeperClientPort) {
      return (CassandraKijiURIBuilder) super.withZookeeperClientPort(zookeeperClientPort);
    }

    /**
     * Configures the CassandraKijiURI with the Kiji instance name.
     *
     * @param instanceName The Kiji instance name.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder withInstanceName(String instanceName) {
      return (CassandraKijiURIBuilder) super.withInstanceName(instanceName);
    }

    /**
     * Configures the CassandraKijiURI with the Kiji table name.
     *
     * @param tableName The Kiji table name.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder withTableName(String tableName) {
      return (CassandraKijiURIBuilder) super.withTableName(tableName);
    }

    /**
     * Configures the CassandraKijiURI with the Kiji column names.
     *
     * @param columnNames The Kiji column names to configure.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder withColumnNames(Collection<String> columnNames) {
      return (CassandraKijiURIBuilder) super.withColumnNames(columnNames);
    }

    /**
     * Adds the column names to the CassandraKijiURI column names.
     *
     * @param columnNames The Kiji column names to add.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder addColumnNames(Collection<KijiColumnName> columnNames) {
      return (CassandraKijiURIBuilder) super.addColumnNames(columnNames);
    }

    /**
     * Adds the column name to the CassandraKijiURI column names.
     *
     * @param columnName The Kiji column name to add.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder addColumnName(KijiColumnName columnName) {
      return (CassandraKijiURIBuilder) super.addColumnName(columnName);
    }

    /**
     * Configures the CassandraKijiURI with the Kiji column names.
     *
     * @param columnNames The Kiji column names.
     * @return This builder instance so you may chain configuration method calls.
     */
    public CassandraKijiURIBuilder withColumnNames(Iterable<KijiColumnName> columnNames) {
      return (CassandraKijiURIBuilder) super.withColumnNames(columnNames);
    }

    /**
     * Builds the configured CassandraKijiURI.
     *
     * @return A KijiURI.
     * @throws org.kiji.schema.KijiURIException If the KijiURI was configured improperly.
     */
    public CassandraKijiURI build() {
      return new CassandraKijiURI(
          mZookeeperQuorum,
          mZookeeperClientPort,
          mCassandraNodes,
          mCassandraClientPort,
          mInstanceName,
          mTableName,
          mColumnNames);
    }
  }

  /**
   * Gets a builder configured with default Kiji URI fields.
   *
   * More precisely, the following defaults are initialized:
   * <ul>
   *   <li>The Zookeeper quorum and client port is taken from the Hadoop <tt>Configuration</tt></li>
   *   <li>The Kiji instance name is set to <tt>KConstants.DEFAULT_INSTANCE_NAME</tt>
   *       (<tt>"default"</tt>).</li>
   *   <li>The table name and column names are explicitly left unset.</li>
   * </ul>
   *
   * @return A builder configured with this Kiji URI.
   */
  public static CassandraKijiURIBuilder newBuilder() {
    return new CassandraKijiURIBuilder();
  }

  /**
   * Gets a builder configured with a Kiji URI.
   *
   * @param uri The Kiji URI to configure the builder from.
   * @return A builder configured with uri.
   */
  public static CassandraKijiURIBuilder newBuilder(CassandraKijiURI uri) {
    return new CassandraKijiURIBuilder(
        uri.getZookeeperQuorumOrdered(),
        uri.getZookeeperClientPort(),
        uri.getCassandraNodesOrdered(),
        uri.getCassandraClientPort(),
        uri.getInstance(),
        uri.getTable(),
        uri.getColumnsOrdered());
  }

  /**
   * Gets a builder configured with the Kiji URI.
   *
   * <p> The String parameter can be a relative URI (with a specified instance), in which
   *     case it is automatically normalized relative to DEFAULT_HBASE_URI.
   *
   * @param uri String specification of a Kiji URI.
   * @return A builder configured with uri.
   * @throws org.kiji.schema.KijiURIException If the uri is invalid.
   */
  public static CassandraKijiURIBuilder newBuilder(String uri) {
    if (!uri.startsWith("kiji-cassandra://")) {
      uri = String.format("%s/%s/", KConstants.DEFAULT_CASSANDRA_URI, uri);
    }
    try {
      return newBuilder(new CassandraKijiURI(new URI(uri)));
    } catch (URISyntaxException exn) {
      throw new KijiURIException(uri, exn.getMessage());
    }
  }
  /**
   * Returns the set of C* hosts (names or IPs).
   *
   * <p> Host names or IP addresses are de-duplicated and sorted. </p>
   *
   * @return the set of C* hosts (names or IPs).  Never null.
   */
  public ImmutableList<String> getCassandraNodes() {
    return mCassandraNodesNormalized;
  }

  /**
   * Returns the original user-specified list of C* hosts.
   *
   * <p> Host names are exactly as specified by the user. </p>
   *
   * @return the original user-specified list of C* hosts.  Never null.
   */
  public ImmutableList<String> getCassandraNodesOrdered() {
    return mCassandraNodes;
  }

  /** @return Cassandra client port. */
  public int getCassandraClientPort() {
    return mCassandraClientPort;
  }

  /**
   * Resolve the path relative to this KijiURI. Returns a new instance.
   *
   * @param path The path to resolve.
   * @return The resolved KijiURI.
   * @throws KijiURIException If this KijiURI is malformed.
   */
  public CassandraKijiURI resolve(String path) {
    try {
      // Without the "./", URI will assume a path containing a colon
      // is a new URI, for example "family:column".
      URI uri = new URI(toString()).resolve(String.format("./%s", path));
      return new CassandraKijiURI(uri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("CassandraKijiURI was incorrectly constructed (should never happen): %s",
              this.toString()));
    } catch (IllegalArgumentException e) {
      throw new KijiURIException(this.toString(),
          String.format("Path can not be resolved: %s", path));
    }
  }

  /**
   * Returns a string representation of this URI.
   *
   * @param preserveOrdering Whether to preserve ordering of lsits in fields.
   * @return A string reprresentation of this URI.
   */
  private String toString(boolean preserveOrdering) {
    // Remove trailing unset fields.
    if (!getColumns().isEmpty()) {
      return toStringCol(preserveOrdering);
    } else if (getTable() != null) {
      return toStringTable(preserveOrdering);
    } else if (getInstance() != null) {
      return toStringInstance(preserveOrdering);
    } else {
      return toStringAuthority(preserveOrdering);
    }
  }

  /**
   * Formats the full CassandraKijiURI up to the authority, preserving order.
   *
   * @param preserveOrdering Whether to preserve ordering.
   * @return Representation of this KijiURI up to the authority.
   */
  String toStringAuthority(boolean preserveOrdering) {
    String zkQuorum;
    ImmutableList<String> zookeeperQuorum =
        preserveOrdering ? getZookeeperQuorumOrdered() : getZookeeperQuorum();
    if (null == zookeeperQuorum) {
      zkQuorum = UNSET_URI_STRING;
    } else {
      if (zookeeperQuorum.size() == 1) {
        zkQuorum = zookeeperQuorum.get(0);
      } else {
        zkQuorum = String.format("(%s)", Joiner.on(",").join(zookeeperQuorum));
      }
    }

    String cNodes;
    ImmutableList<String> cassandraNodes =
        preserveOrdering ? getCassandraNodesOrdered() : getCassandraNodes();
    if (null == cassandraNodes) {
      cNodes = UNSET_URI_STRING;
    } else {
      if (cassandraNodes.size() == 1) {
        cNodes = cassandraNodes.get(0);
      } else {
        cNodes = String.format("(%s)", Joiner.on(",").join(cassandraNodes));
      }
    }

    return String.format("%s://%s:%s/%s/%s/",
        KIJI_SCHEME,
        zkQuorum,
        getZookeeperClientPort(),
        cNodes,
        getCassandraClientPort());
  }

  /**
   * {@inheritDoc}
   */
  public boolean isCassandra() { return true; }
}
