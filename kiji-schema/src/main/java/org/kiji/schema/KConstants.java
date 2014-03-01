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

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/** Constants used by Kiji. */
@ApiAudience.Framework
@ApiStability.Evolving
public final class KConstants {
  /** Default kiji instance name. */
  public static final String DEFAULT_INSTANCE_NAME = "default";

  /** Default Kiji URI, pointing to the default HBase cluster. */
  public static final String DEFAULT_HBASE_URI = "kiji://.env";

  /** Default Cassandra URI. */
  public static final String DEFAULT_CASSANDRA_URI = "kiji-cassandra://.env/localhost/9160";

  /** Default Kiji URI with instance specified as 'default'. */
  public static final String DEFAULT_INSTANCE_URI =
      String.format("%s/%s", DEFAULT_HBASE_URI, DEFAULT_INSTANCE_NAME);

  /** Default Kiji URI, as built by KijiURI.newBuilder().build(). */
  public static final String DEFAULT_URI = DEFAULT_INSTANCE_URI;

  public static final long END_OF_TIME = Long.MAX_VALUE;
  public static final long BEGINNING_OF_TIME = 0;

  public static final long CASSANDRA_COUNTER_TIMESTAMP = Long.MAX_VALUE;

  /** Utility classes cannot be instantiated. */
  private KConstants() {
  }
}
