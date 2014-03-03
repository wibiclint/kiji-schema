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

import org.junit.*;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/** Simple read/write tests. */
public class TestCassandraKijiBufferedWriter extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraKijiBufferedWriter.class);

  private static KijiTable mTable;

  private Kiji mKiji;
  private KijiBufferedWriter mBufferedWriter;
  private KijiTableReader mReader;
  private KijiTableWriter mWriter;

  /** Use to create unique entity IDs for each test case. */
  private static AtomicInteger testIdCounter;

  /** Unique per test case -- keep tests on different rows. */
  private EntityId mEntityId;

  @Before
  public final void setupEnvironment() throws Exception {
    // Fill local variables.
    mBufferedWriter = mTable.getWriterFactory().openBufferedWriter();
    mReader = mTable.openTableReader();
    mWriter = mTable.openTableWriter();
    mEntityId = mTable.getEntityId("eid-" + testIdCounter.getAndIncrement());
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mBufferedWriter.close();
    mReader.close();
    mWriter.close();
  }

  @BeforeClass
  public static void initShared() {
    CassandraKijiClientTest clientTest = new CassandraKijiClientTest();
    testIdCounter = new AtomicInteger(0);
    try {
      clientTest.setupKijiTest();
      Kiji kiji = clientTest.getKiji();
      kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

      mTable = kiji.openTable("user");
      /*
      final EntityId eid = mTable.getEntityId("me");
      final KijiTableWriter writer = mTable.openTableWriter();
      try {
        writer.put(eid, "info", "name", 1L, "me-one");
        writer.put(eid, "info", "name", 2L, "me-two");
        writer.put(eid, "info", "name", 3L, "me-three");
        writer.put(eid, "info", "name", 4L, "me-four");
        writer.put(eid, "info", "name", 5L, "me-five");

        for (int job = 0; job < NJOBS; ++job) {
          for (long ts = 1; ts <= NTIMESTAMPS; ++ts) {
            writer.put(eid, "jobs", String.format("j%d", job), ts, String.format("j%d-t%d", job, ts));
          }
        }

      } finally {
        writer.close();
      }
        */
    } catch (Exception e) {
      throw new KijiIOException(e);
    }

  }

  @AfterClass
  public static void cleanupClass() throws IOException {
    mTable.release();
  }

  @Test
  public void testBasicReadAndWrite() throws Exception {
    mBufferedWriter.put(mEntityId, "info", "name", 0L, "Value at timestamp 0.");
    mBufferedWriter.put(mEntityId, "info", "name", 1L, "Value at timestamp 1.");

    // These have not been flushed yet, so should not be present.
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(10).add("info", "name"))
        .build();

    KijiRowData rowData;
    rowData = mReader.get(mEntityId, dataRequest);
    assertFalse(rowData.containsCell("info", "name", 0L));

    // Now do a flush.
    mBufferedWriter.flush();

    // If you read again, the data should be present.

    // Try this as a get.
    rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue("info", "name", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");
  }


  @Test
  public void testPutWithTimestamp() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Write a value now.
    mWriter.put(mEntityId, "info", "name", 123L, "old");

    // Buffer the new value and confirm it has not been written.
    mBufferedWriter.put(mEntityId, "info", "name", 123L, "baz");
    final String actual = mReader.get(mEntityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Flush the buffer and confirm the new value has been written.
    mBufferedWriter.flush();
    final String actual2 = mReader.get(mEntityId, request).getValue("info", "name", 123L).toString();
    assertEquals("baz", actual2);
  }

  @Test
  public void testSetCounter() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");

    // Cannot set a counter from a Cassandra buffered writer.
    try {
      mBufferedWriter.put(mEntityId, "info", "visits", 5L);
      fail("Exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testDeleteColumn() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Write initial value.
    mWriter.put(mEntityId, "info", "name", 123L, "not empty");

    // Buffer the delete and confirm it has not happened yet.
    assertTrue(mReader.get(mEntityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteColumn(mEntityId, "info", "name");
    assertTrue(mReader.get(mEntityId, request).containsCell("info", "name", 123L));

    // Flush the buffer and confirm the delete has happened.
    mBufferedWriter.flush();
    assertFalse(mReader.get(mEntityId, request).containsCell("info", "name", 123L));
  }

  @Test
  public void testDeleteCell() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Write initial value.
    mWriter.put(mEntityId, "info", "name", 123L, "not empty");

    // Buffer the delete and confirm it has not happened yet.
    assertTrue(mReader.get(mEntityId, request).containsCell("info", "name", 123L));
    mBufferedWriter.deleteCell(mEntityId, "info", "name", 123L);
    final String actual = mReader.get(mEntityId, request).getValue("info", "name", 123L).toString();
    assertEquals("not empty", actual);

    // Flush the buffer and confirm the delete has happened.
    mBufferedWriter.flush();
    assertFalse(mReader.get(mEntityId, request).containsCell("info", "name", 123L));
  }

  @Test
  public void testDeleteCellNoTimestamp() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Write initial value.
    mWriter.put(mEntityId, "info", "name", 123L, "not empty");

    // Buffer the delete and confirm it has not happened yet.
    assertTrue(mReader.get(mEntityId, request).containsCell("info", "name", 123L));

    // Cannot delete most-recent version of a cell in Cassandra Kiji.
    try {
      mBufferedWriter.deleteCell(mEntityId, "info", "name");
      fail("Exception should occur.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testSetBufferSize() throws Exception {
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // TODO: Remove duplicate puts from test when buffered writer calculates buffer size correctly.
    // (Right now the buffer size is measured in C* Statements, not bytes.)

    // Add a put to the buffer.
    mBufferedWriter.put(mEntityId, "info", "name", 123L, "old");
    mBufferedWriter.put(mEntityId, "info", "name", 123L, "old");
    assertFalse(mReader.get(mEntityId, request).containsCell("info", "name", 123L));

    // Shrink the buffer, pushing the buffered put.
    mBufferedWriter.setBufferSize(1L);
    final String actual = mReader.get(mEntityId, request).getValue("info", "name", 123L).toString();
    assertEquals("old", actual);

    // Add a put which should commit immediately.
    mBufferedWriter.put(mEntityId, "info", "name", 234L, "new");
    mBufferedWriter.put(mEntityId, "info", "name", 234L, "new");
    final String actual2 = mReader.get(mEntityId, request).getValue("info", "name", 234L).toString();
    assertEquals("new", actual2);
  }

  @Test
  public void testBufferPutWithDelete() throws Exception {
    final EntityId oldEntityId = mTable.getEntityId("foo");
    final EntityId newEntityId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Initialize data for the old entity ID.
    mWriter.put(oldEntityId, "info", "name", "foo-name");

    // Buffer a delete for "foo" and a put to "bar" and confirm they have not been written.
    mBufferedWriter.deleteRow(oldEntityId);
    mBufferedWriter.put(newEntityId, "info", "name", "bar-name");
    assertTrue(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertFalse(mReader.get(newEntityId, request).containsColumn("info", "name"));

    // Flush the buffer and ensure delete and put have been written
    mBufferedWriter.flush();
    assertFalse(mReader.get(oldEntityId, request).containsColumn("info", "name"));
    assertTrue(mReader.get(newEntityId, request).containsColumn("info", "name"));
  }

}
