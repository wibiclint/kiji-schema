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

package org.kiji.schema.tools;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.impl.cassandra.CassandraKiji;
import org.kiji.schema.impl.cassandra.CassandraKijiFactory;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.SplitKeyFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Command-line tool for creating C*-backed kiji tables in C*-backed kiji instances.
 */
@ApiAudience.Private
public final class CassandraCreateTableTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraCreateTableTool.class);

  @Flag(name="table", usage="URI of the Kiji table to create,"
      + " eg. --table=kiji://cassandra-address/kiji-instance/table.")
  private String mTableURIFlag = null;

  @Flag(name="layout", usage="Path to a file containing a JSON table layout description.")
  private String mLayout = null;

  /** Opened Kiji to use. */
  private Kiji mKiji;

  /** KijiURI of the table to create. */
  private KijiURI mTableURI;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "cassandra-create-table";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Create a Cassandra-backed kiji table in a Cassandra-backed kiji instance.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "DDL";
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    Preconditions.checkArgument((mTableURIFlag != null) && !mTableURIFlag.isEmpty(),
        "Specify the table to create with --table=kiji://hbase-address/kiji-instance/table");
    mTableURI = KijiURI.newBuilder(mTableURIFlag).build();

    Preconditions.checkArgument((mLayout != null) && !mLayout.isEmpty(),
        "Specify the table layout with --layout=/path/to/table-layout.json");
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    mKiji = (new CassandraKijiFactory()).open(mTableURI, getConf());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    mKiji.release();
    mKiji = null;
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    getPrintStream().println("Parsing table layout: " + mLayout);
    final Path path = new Path(mLayout);
    final FileSystem fs =
        fileSystemSpecified(path) ? path.getFileSystem(getConf()) : FileSystem.getLocal(getConf());
    final FSDataInputStream inputStream = fs.open(path);
    final TableLayoutDesc tableLayout = KijiTableLayout.readTableLayoutDescFromJSON(inputStream);
    final String tableName = tableLayout.getName();
    Preconditions.checkArgument(
        (mTableURI.getTable() == null) || tableName.equals(mTableURI.getTable()),
        "Table name '%s' does not match URI %s", tableName, mTableURI);

    getPrintStream().println("Creating Kiji table " + mTableURI);
    mKiji.createTable(tableLayout);
    return SUCCESS;
  }

  /**
   * Determines whether a path has its filesystem explicitly specified.  Did it start
   * with "hdfs://" or "file://"?
   *
   * @param path The path to check.
   * @return Whether a file system was explicitly specified in the path.
   */
  private static boolean fileSystemSpecified(Path path) {
    return null != path.toUri().getScheme();
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new CassandraCreateTableTool(), args));
  }
}
