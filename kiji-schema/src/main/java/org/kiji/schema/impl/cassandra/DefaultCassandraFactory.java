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

package org.kiji.schema.impl.cassandra;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.kiji.annotations.ApiAudience;
import org.kiji.delegation.Priority;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraFactory;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.util.LockFactory;
import org.kiji.schema.util.ZooKeeperLockFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Factory for HBase instances based on URIs. */
@ApiAudience.Private
public final class DefaultCassandraFactory implements CassandraFactory {

  /**
   * Public constructor for us by the service loader. Clients should use
   * HBaseFactory.Provider.get(), which maintains a singleton instance.
   */
  public DefaultCassandraFactory() {
  }

  /** {@inheritDoc} */
  @Override
  public CassandraAdminFactory getCassandraAdminFactory(KijiURI uri) {
    return DefaultCassandraAdminFactory.get();
  }

  /** {@inheritDoc} */
  @Override
  public LockFactory getLockFactory(KijiURI uri, Configuration conf) throws IOException {
    return new ZooKeeperLockFactory(ZooKeeperLockFactory.zkConnStr(uri));
  }

  /** {@inheritDoc} */
  @Override
  public int getPriority(Map<String, String> runtimeHints) {
    // Default priority; should be used unless overridden by tests.
    return Priority.NORMAL;
  }

  /** {@inheritDoc} */
  @Override
  public ZooKeeperClient getZooKeeperClient(final KijiURI uri) {
    final List<String> zkHosts = Lists.newArrayList();
    for (String host : uri.getZookeeperQuorumOrdered()) {
      zkHosts.add(String.format("%s:%s", host, uri.getZookeeperClientPort()));
    }
    final String zkAddress = Joiner.on(",").join(zkHosts);
    final int sessionTimeoutMS = 60 * 1000;
    final ZooKeeperClient zkc = new ZooKeeperClient(zkAddress, sessionTimeoutMS);
    zkc.open();
    return zkc;
  }
}
