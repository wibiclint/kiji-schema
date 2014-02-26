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

import org.junit.Test;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayouts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Simple read/write tests. */
public class TestCassandraKijiBufferedWriter extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraKijiBufferedWriter.class);

  @Test
  public void testBasicReadAndWrite() throws Exception {
    LOG.info("Opening an in-memory kiji instance");
    final Kiji kiji = getKiji();

    LOG.info(String.format("Opened fake Kiji '%s'.", kiji.getURI()));
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));

    final KijiTable table = kiji.openTable("table");
    final KijiBufferedWriter writer = table.getWriterFactory().openBufferedWriter();
    final KijiTableReader reader = table.openTableReader();

    EntityId eid0 = table.getEntityId("row0");

    writer.put(eid0, "family", "column", 0L, "Value at timestamp 0.");
    writer.put(eid0, "family", "column", 1L, "Value at timestamp 1.");

    // These have not been flushed yet, so should not be present.

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(10).add("family", "column"))
        .build();

    KijiRowData rowData;
    rowData = reader.get(eid0, dataRequest);
    assertFalse(rowData.containsCell("family", "column", 0L));

    // Now do a flush.
    writer.flush();

    // If you read again, the data should be present.

    // Try this as a get.
    rowData = reader.get(eid0, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");

    reader.close();
    writer.close();
    kiji.release();
  }
}
