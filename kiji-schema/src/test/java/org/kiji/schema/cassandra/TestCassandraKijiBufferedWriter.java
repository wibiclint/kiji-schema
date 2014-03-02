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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.*;

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


  private Kiji mKiji;
  private KijiTable mTable;
  private KijiBufferedWriter mBufferedWriter;
  private KijiTableReader mReader;

  @Before
  public final void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder(getKiji())
        .withTable("user", layout)
          .withRow("foo")
            .withFamily("info")
                .withQualifier("name").withValue(1L, "foo-val")
                //.withQualifier("visits").withValue(1L, 42L)
          .withRow("bar")
            .withFamily("info")
                //.withQualifier("visits").withValue(1L, 100L)
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mBufferedWriter = mTable.getWriterFactory().openBufferedWriter();
    mReader = mTable.openTableReader();
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mBufferedWriter.close();
    mReader.close();
    mTable.release();
  }

  @Test
  public void testPutWithTimestamp() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "old");
    writer.close();
    // Buffer the new value and confirm it has not been written.
    mBufferedWriter.put(entityId, "info", "name", 123L, "baz");
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Flush the buffer and confirm the new value has been written.
    mBufferedWriter.flush();
    final String actual2 = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("baz", actual2);
  }

  @Test
  public void testSetCounter() throws Exception {
    final EntityId entityId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "visits", 1L);
    writer.close();

    // Cannot set a counter from a Cassandra buffered writer.
    try {
      mBufferedWriter.put(entityId, "info", "visits", 5L);
      fail("Exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testDeleteColumn() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "not empty");
    writer.close();

    // Buffer the delete and confirm it has not been written.
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteColumn(entityId, "info", "name");
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));

    // Flush the buffer and confirm the delete has been written.
    mBufferedWriter.flush();
    assertFalse(mReader.get(entityId, request).containsCell("info", "name", 123L));
  }

  @Test
  public void testDeleteCell() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "not empty");
    writer.close();

    // Buffer the delete and confirm it has not been written.
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteCell(entityId, "info", "name", 123L);
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("not empty", actual);

    // Flush the buffer and confirm the delete has been written.
    mBufferedWriter.flush();
    assertFalse(mReader.get(entityId, request).containsCell("info", "name", 123L));
  }

  @Test
  public void testDeleteCellNoTimestamp() throws Exception {
    final EntityId entityId = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Prepare the old value.
    KijiTableWriter writer = mTable.openTableWriter();
    writer.put(entityId, "info", "name", 123L, "not empty");
    writer.close();

    // Buffer the delete and confirm it has not been written.
    assertTrue(mReader.get(entityId, request).containsCell("info", "name", 123L));
    try {
      mBufferedWriter.deleteCell(entityId, "info", "name");
      fail("Exception should occurr.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testSetBufferSize() throws Exception {
    final EntityId entityId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // TODO: Remove duplicate puts from test when buffered writer calculates buffer size correctly.
    // (Right now the buffer size is measured in C* Statements, not bytes.)

    // Add a put to the buffer.
    mBufferedWriter.put(entityId, "info", "name", 123L, "old");
    mBufferedWriter.put(entityId, "info", "name", 123L, "old");
    assertFalse(mReader.get(entityId, request).containsCell("info", "name", 123L));

    // Shrink the buffer, pushing the buffered put.
    mBufferedWriter.setBufferSize(1L);
    final String actual = mReader.get(entityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Add a put which should commit immediately.
    mBufferedWriter.put(entityId, "info", "name", 234L, "new");
    mBufferedWriter.put(entityId, "info", "name", 234L, "new");
    final String actual2 = mReader.get(entityId, request).getValue("info", "name", 234L).toString();
    assertEquals("new", actual2);
  }

  @Test
  public void testBufferPutWithDelete() throws Exception {
    final EntityId oldEntityId = mTable.getEntityId("foo");
    final EntityId newEntityId = mTable.getEntityId("blope");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Buffer a delete for "foo" and a put to "blope" and confirm they have not been written.
    mBufferedWriter.deleteRow(oldEntityId);
    mBufferedWriter.put(newEntityId, "info", "name", "blopeName");
    assertTrue(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertFalse(mReader.get(newEntityId, request).containsColumn("info", "name"));

    // Flush the buffer and ensure delete and put have been written
    mBufferedWriter.flush();
    assertFalse(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertTrue(mReader.get(newEntityId, request).containsColumn("info", "name"));
  }

}
