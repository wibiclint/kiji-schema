/**
 * (c) Copyright 2014 WibiData, Inc.
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

import org.apache.ftpserver.command.impl.USER;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert.*;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/** Collection of different tests for C* counters. */
public class TestCassandraCounters extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraCounters.class);

  private static final String MAP = "experiments";
  private static final String Q0 = "q0";
  private static final String USERNAME = "Mr Bonkers";

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiBufferedWriter mBuffered;
  private KijiTableReader mReader;
  private AtomicKijiPutter mPutter;
  private EntityId mEntityId;

  @Before
  public final void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    // info:name is a string
    // info:visits is a counter
    // experiments is a map family of counters.
    mKiji = new InstanceBuilder(getKiji())
        .withTable("user", layout)
        .withRow("foo")
        .withFamily("info")
        .withQualifier("name").withValue(1L, USERNAME)
        //.withQualifier("visits").withValue(42L)
        .withRow("bar")
        .withFamily("info")
        //.withQualifier("visits").withValue(100L)
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mWriter = mTable.openTableWriter();
    mReader = mTable.openTableReader();
    mEntityId = mTable.getEntityId("foo");
    mBuffered = mTable.getWriterFactory().openBufferedWriter();
    mPutter = mTable.getWriterFactory().openAtomicPutter();
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mWriter.close();
    mReader.close();
    mTable.release();
    mPutter.close();
    mBuffered.close();
  }

  // Test incrementing a counter that has already been initialized.
  @Test
  public void testIncrementInitialized() throws Exception {
    mWriter.put(mEntityId, "info", "visits", 42L);

    KijiCell<Long> incrementResult = mWriter.increment(mEntityId, "info", "visits", 5L);
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");
    final long postIncrementValue = incrementResult.getData();
    assertEquals(47L, postIncrementValue);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, incrementResult.getTimestamp());


    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell("info", "visits");
    final long actual = counter.getData();
    assertEquals(47L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
  }

  // Test incrementing a counter that has not yet been initialized.
  @Test
  public void testIncrementUninitialized() throws Exception {
    mWriter.increment(mEntityId, MAP, Q0, 1L);
    final KijiDataRequest request = KijiDataRequest.create(MAP, Q0);
    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    final long actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
  }

  // Test setting a counter.
  @Test
  public void testSet() throws Exception {
    mWriter.put(mEntityId, "info", "visits", 5L);
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");
    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell("info", "visits");
    assertNotNull(counter);
    final long actual = counter.getData();
    assertEquals(5L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
  }

  // TODO: Test that setting a counter with a specific timestamp fails.

  // TODO: Test that setting a counter in a compare-and-set fails.

  // Test that we can delete a counter with a writer.
  @Test
  public void testDelete() throws Exception {
    mWriter.increment(mEntityId, MAP, Q0, 1L);
    final KijiDataRequest request = KijiDataRequest.create(MAP, Q0);
    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    long actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());

    mWriter.deleteCell(mEntityId, MAP, Q0);
    assertFalse(mReader
        .get(mEntityId, request)
        .containsCell(MAP, Q0, KConstants.CASSANDRA_COUNTER_TIMESTAMP));

    mWriter.increment(mEntityId, MAP, Q0, 1L);
    counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
  }

  // Test setting and deleting with a buffered writer.
  @Test
  public void testBuffered() throws Exception {
    // Buffered writer can write and read counters, but not increment them.
    mBuffered.put(mEntityId, MAP, Q0, 1L);
    mBuffered.flush();
    final KijiDataRequest request = KijiDataRequest.create(MAP, Q0);
    KijiCell<Long> counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    long actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());

    mBuffered.deleteCell(mEntityId, MAP, Q0);
    // No flush yet, data should still be there:
    counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());

    // Flush, data is gone.
    mBuffered.flush();
    assertFalse(mReader
        .get(mEntityId, request)
        .containsCell(MAP, Q0, KConstants.CASSANDRA_COUNTER_TIMESTAMP));

    mBuffered.put(mEntityId, MAP, Q0, 1L);

    // No flush, data still missing.
    assertFalse(mReader
        .get(mEntityId, request)
        .containsCell(MAP, Q0, KConstants.CASSANDRA_COUNTER_TIMESTAMP));

    // Flush, data present again.
    mBuffered.flush();
    counter = mReader.get(mEntityId, request).getMostRecentCell(MAP, Q0);
    actual = counter.getData();
    assertEquals(1L, actual);
    assertEquals(KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter.getTimestamp());
  }

  @Test
  public void testAtomicPutter() throws Exception {
    // Buffered writer can write and read counters, but not increment them.
    try {
      mPutter.begin(mEntityId);
      mPutter.put(MAP, Q0, 1L);
      org.junit.Assert.fail("An exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  @Test
  public void testIncrementNonCounter() throws Exception {
    try {
      mWriter.increment(mEntityId, "info", "name", 1L);
      org.junit.Assert.fail("An exception should have occurred.");
    } catch (UnsupportedOperationException e) {
      assertNotNull(e);
    }
  }

  // Test reading a family with counters and non-counters.
  @Test
  public void testReadMixedFamily() throws Exception {
    mWriter.put(mEntityId, "info", "visits", 42L);
    final KijiDataRequest dataRequest = KijiDataRequest.create("info");
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);

    KijiCell<Long> counter = rowData.getMostRecentCell("info", "visits");
    KijiCell<CharSequence> name = rowData.getMostRecentCell("info", "name");

    assertNotNull(counter);
    long counterValue = counter.getData();
    assertEquals(42L, counterValue);
    assertNotNull(name);
    assertEquals(USERNAME, name.getData());
  }

  // Test reading multiple families, some with counters, some without.
  @Test
  public void testReadMultipleFamilies() throws Exception {
    // Initialize a counter in the map-type family.
    mWriter.increment(mEntityId, MAP, Q0, 1L);

    // Initialize a counter in the group-type family.
    mWriter.put(mEntityId, "info", "visits", 42L);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily(MAP))
        .addColumns(ColumnsDef.create().addFamily("info"))
        .build();

    KijiRowData rowData = mReader.get(mEntityId, dataRequest);

    KijiCell<Long> counter = rowData.getMostRecentCell("info", "visits");
    KijiCell<CharSequence> name = rowData.getMostRecentCell("info", "name");
    KijiCell<Long> counterMap = rowData.getMostRecentCell(MAP, Q0);

    assertNotNull(counter);
    long counterValue = counter.getData();
    assertEquals(42L, counterValue);

    assertNotNull(name);
    assertEquals(USERNAME, name.getData());

    assertNotNull(counterMap);
    counterValue = counterMap.getData();
    assertEquals(1L, counterValue);

  }

  // TODO: Read a counter in a table scan.

  // TODO: ead a counter with paging

  // TODO: Read a counter with scanner + paging.

  // Try doing lots of increments in parallel?



}
