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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.Assert.*;

/** Basic test for creating a simple Kiji table in Cassandra. */
public class TestCassandraCreateKijiTable extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraCreateKijiTable.class);

  @Test
  public void testCreateKijiTable() throws Exception {
    LOG.info("Opening an in-memory kiji instance");
    final Kiji kiji = getKiji();

    try {
      LOG.info(String.format("Opened fake Kiji '%s'.", kiji.getURI()));

      final KijiSystemTable systemTable = kiji.getSystemTable();
      assertTrue("Client data version should support installed Kiji instance data version",
          VersionInfo.getClientDataVersion().compareTo(systemTable.getDataVersion()) >= 0);

      assertNotNull(kiji.getSchemaTable());
      assertNotNull(kiji.getMetaTable());

      kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));


    } finally {
      //kiji.release();
    }
  }
    /*
      final KijiTable table = kiji.openTable("table");
      try {
        {
          final KijiTableReader reader = table.openTableReader();
          try {
            final KijiDataRequest dataRequest = KijiDataRequest.builder()
                .addColumns(ColumnsDef.create().addFamily("family"))
                .build();
            final KijiRowScanner scanner = reader.getScanner(dataRequest);
            try {
              assertFalse(scanner.iterator().hasNext());
            } finally {
              scanner.close();
            }
          } finally {
            reader.close();
          }
        }

        {
          final KijiTableWriter writer = table.openTableWriter();
          try {
            writer.put(table.getEntityId("row1"), "family", "column", "the string value");
          } finally {
            writer.close();
          }
        }

        {
          final KijiTableReader reader = table.openTableReader();
          try {
            final KijiDataRequest dataRequest = KijiDataRequest.builder()
                .addColumns(ColumnsDef.create().addFamily("family"))
                .build();
            final KijiRowScanner scanner = reader.getScanner(dataRequest);
            try {
              final Iterator<KijiRowData> it = scanner.iterator();
              assertTrue(it.hasNext());
              KijiRowData row = it.next();
              assertEquals("the string value",
                  row.getMostRecentValue("family", "column").toString());
              assertFalse(it.hasNext());
            } finally {
              scanner.close();
            }
          } finally {
            reader.close();
          }
        }
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }
  */
}
