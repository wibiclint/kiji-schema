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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.avro.*;
import org.kiji.schema.impl.AvroCellEncoder;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.impl.cassandra.CassandraDataRequestAdapter;
import org.kiji.schema.impl.cassandra.CassandraKijiRowData;
import org.kiji.schema.impl.cassandra.CassandraKijiTable;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;
import org.kiji.schema.util.InstanceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestCassandraKijiRowData extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraKijiRowData.class);

  /** Test layout. */
  public static final String TEST_LAYOUT_V1 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.test-layout-v1.json";

  /** Update for TEST_LAYOUT, with Test layout with column "family:qual0" removed. */
  public static final String TEST_LAYOUT_V2 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.test-layout-v2.json";

  /** Layout for table 'writer_schema' to test when a column class is not found. */
  public static final String WRITER_SCHEMA_TEST =
      "org/kiji/schema/layout/TestHBaseKijiRowData.writer-schema.json";

  /** Test layout with version layout-1.3. */
  public static final String TEST_LAYOUT_V1_3 =
      "org/kiji/schema/layout/TestHBaseKijiRowData.layout-v1.3.json";

  private static final String TABLE_NAME = "row_data_test_table";

  private static String FAMILY = "family";
  private static String EMPTY = "empty";
  private static String QUAL0 = "qual0";
  private static String QUAL1 = "qual1";
  private static String QUAL2 = "qual2";
  private static String QUAL3 = "qual3";
  private static String NODEQUAL0 = "nodequal0";
  private static String NODEQUAL1 = "nodequal1";
  private static String MAP = "map";
  private static String KEY0 = "key0";
  private static String KEY1 = "key1";
  private static String KEY2 = "key2";

  private final static int KEY0_VAL = 100;
  private final static int KEY1_VAL = 101;

  private EntityIdFactory mEntityIdFactory;

  /** KijiTable used for some tests (named TABLE_NAME). */
  private static KijiTable mTable;

  private static final Node mNode0 = Node.newBuilder().setLabel("node0").build();
  private static final Node mNode1 = Node.newBuilder().setLabel("node1").build();

  /** Use to create unique entity IDs for each test case. */
  private static AtomicInteger testIdCounter;

  /** Unique per test case -- keep tests on different rows. */
  private EntityId mEntityId;
  private KijiTableReader mReader;
  private KijiTableWriter mWriter;

  @BeforeClass
  public static void initShared() {
    CassandraKijiClientTest clientTest = new CassandraKijiClientTest();
    testIdCounter = new AtomicInteger(0);
    try {
      clientTest.setupKijiTest();
      Kiji kiji = clientTest.getKiji();
      kiji.createTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1));

      mTable = kiji.openTable(TABLE_NAME);
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

  @Before
  public final void setupEnvironment() throws Exception {
    // Fill local variables.
    mReader = mTable.openTableReader();
    mWriter = mTable.openTableWriter();
    mEntityId = mTable.getEntityId("eid-" + testIdCounter.getAndIncrement());
  }

  @After
  public final void tearDownTestHBaseKijiRowData() throws Exception {
    mReader.close();
    mWriter.close();
  }

  @AfterClass
  public static void closeOut() throws IOException {
    mTable.release();
  }

  // -----------------------------------------------------------------------------------------------
  // Test cases that need to interact with an actual Kiji table.

  @Test
  public void testEntityId() throws Exception {
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add(FAMILY, QUAL0))
        .build();

    // Put some data into the table.
    mWriter.put(mEntityId, FAMILY, QUAL0, Bytes.toBytes("bot"));

    // Read out the results to get a KijiRowData
    final KijiRowData input = mReader.get(mEntityId, dataRequest);
    assertEquals(mEntityId, input.getEntityId());
  }

  @Test
  public void testReadInts() throws Exception {
    // Put some data into the table.
    mWriter.put(mEntityId, FAMILY, QUAL3, 1L, 42);

    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add(FAMILY, QUAL3);
    KijiDataRequest dataRequest = builder.build();

    // Read out the results to get a KijiRowData
    final KijiRowData input = mReader.get(mEntityId, dataRequest);

    assertNotNull(input.getMostRecentValue(FAMILY, QUAL3));
    final int integer = (Integer) input.getMostRecentValue(FAMILY, QUAL3);
    assertEquals(42, integer);
  }

  @Test
  public void testGetReaderSchema() throws Exception {
    // Empty data request.
    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();

    // Read data for an entity ID that does not exist.
    final KijiRowData input = mReader.get(mEntityId, dataRequest);

    assertEquals(Schema.create(Schema.Type.STRING), input.getReaderSchema("family", "empty"));
    assertEquals(Schema.create(Schema.Type.INT), input.getReaderSchema("family", "qual3"));
  }

  @Test
  public void testGetReaderSchemaNoSuchColumn() throws Exception {
    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();

    // Read data for an entity ID that does not exist.
    final KijiRowData input = mReader.get(mEntityId, dataRequest);

    try {
      input.getReaderSchema("this_family", "does_not_exist");
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'row_data_test_table' has no family 'this_family'.", nsce.getMessage());
    }

    try {
      input.getReaderSchema("family", "no_qualifier");
      fail("An exception should have been thrown.");
    } catch (NoSuchColumnException nsce) {
      assertEquals("Table 'row_data_test_table' has no column 'family:no_qualifier'.",
          nsce.getMessage());
    }
  }

  @Test
  public void testReadMiddleTimestamp() throws IOException {
    // Test that we can select a timestamped value that is not the most recent value.
    mWriter.put(mEntityId, "family", "qual0", 4L, "oldest");
    mWriter.put(mEntityId, "family", "qual0", 6L, "middle");
    mWriter.put(mEntityId, "family", "qual0", 8L, "newest");

    mWriter.put(mEntityId, "family", "qual1", 1L, "one");
    mWriter.put(mEntityId, "family", "qual1", 2L, "two");
    mWriter.put(mEntityId, "family", "qual1", 3L, "three");
    mWriter.put(mEntityId, "family", "qual1", 4L, "four");
    mWriter.put(mEntityId, "family", "qual1", 8L, "eight");

    mWriter.put(mEntityId, "family", "qual2", 3L, "q2-three");
    mWriter.put(mEntityId, "family", "qual2", 4L, "q2-four");
    mWriter.put(mEntityId, "family", "qual2", 6L, "q2-six");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .withTimeRange(2L, 7L)
        .addColumns(ColumnsDef.create().add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .addColumns(ColumnsDef.create().withMaxVersions(3).add("family", "qual2"))
        .build();

    final KijiRowData row1 = mReader.get(mEntityId, dataRequest);

    // This should be "middle" based on the time range of the data request.
    final String qual0val = row1.getMostRecentValue("family", "qual0").toString();
    assertEquals("Didn't get the middle value for family:qual0", "middle", qual0val);

    // We always optimize maxVersions=1 to actually return exactly 1 value, even of
    // we requested more versions of other columns.
    final NavigableMap<Long, CharSequence> q0vals = row1.getValues("family", "qual0");
    assertEquals("qual0 should only return one thing", 1, q0vals.size());
    assertEquals("Newest (only) value in q0 should be 'middle'.",
        "middle", q0vals.firstEntry().getValue().toString());

    // qual1 should see at least two versions, but no newer than 7L.
    final NavigableMap<Long, CharSequence> q1vals = row1.getValues("family", "qual1");
    assertEquals("qual1 getValues should have exactly two items", 2, q1vals.size());
    assertEquals("Newest value in q1 should be 'four'.",
        "four", q1vals.firstEntry().getValue().toString());

    // qual2 should see exactly three versions.
    final NavigableMap<Long, CharSequence> q2vals = row1.getValues("family", "qual2");
    assertEquals("qual2 getValues should have exactly three items", 3, q2vals.size());
    assertEquals("Newest value in q2 should be 'q2-six'.",
        "q2-six", q2vals.firstEntry().getValue().toString());
  }

  @Test
  public void testEmptyResult() throws IOException {
    // TODO: Test having results for a family, but not for a particular qualifier.
    // TODO: Test not having results for family or qualifier.
    mWriter.put(mEntityId, "family", "qual0", 1L, "string1");
    mWriter.put(mEntityId, "family", "qual0", 2L, "string2");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "qual1"))
        .build();

    final KijiRowData row1 = mReader.get(mEntityId, dataRequest);

    final NavigableMap<Long, CharSequence> values = row1.getValues("family", "qual1");
    assertTrue("getValues should return an empty map for empty rowdata.", values.isEmpty());

    final NavigableMap<Long, KijiCell<CharSequence>> cells = row1.getCells("family", "qual1");
    assertTrue("getCells should return an empty map for empty rowdata.", cells.isEmpty());

    final Iterator<KijiCell<CharSequence>> iterator =  row1.iterator("family", "qual1");
    assertFalse("iterator obtained on a column the rowdata has no data for should return false"
        + "when hasNext is called.",
        iterator.hasNext());

    final CharSequence value = row1.getMostRecentValue("family", "qual1");
    assertEquals("getMostRecentValue should return a null value from an empty rowdata.",
        null,
        value);

    final KijiCell<CharSequence> cell = row1.getMostRecentCell("family", "qual1");
    assertEquals("getMostRecentCell should return a null cell from empty rowdata.",
        null,
        cell);
  }

  // This test was created in response to WIBI-41.  If your KijiDataRequest doesn't contain
  // one of the columns in the Result map, you used to a get a NullPointerException.
  @Test
  public void testGetMap() throws Exception {
    // Put some data into the table.
    mWriter.put(mEntityId, FAMILY, QUAL0, "bot");
    mWriter.put(mEntityId, FAMILY, EMPTY, "car");

    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();

    // We didn't request any data, so the map should be null.
    final KijiRowData input = mReader.get(mEntityId, dataRequest);
    assertTrue(((CassandraKijiRowData)input).getMap().isEmpty());
  }

  @Test
  public void testContainsColumn() throws Exception {
    final long TIME = 1L;
    mWriter.put(mEntityId, FAMILY, QUAL0, TIME, "foo");

    KijiRowData row1 = mReader.get(mEntityId, KijiDataRequest.create(FAMILY, QUAL0));
    assertTrue(row1.containsCell(FAMILY, QUAL0, TIME));
    assertFalse(row1.containsCell(FAMILY, QUAL0, TIME+1L));
    assertFalse(row1.containsCell("fake", QUAL0, TIME));
    assertFalse(row1.containsCell(FAMILY, "fake", TIME));
  }

  @Test
  public void testIteratorMapFamilyTypes() throws IOException {
    mWriter.put(mEntityId, MAP, KEY0, 1L, 0);
    mWriter.put(mEntityId, MAP, KEY1, 1L, 1);
    mWriter.put(mEntityId, MAP, KEY2, 1L, 2);
    mWriter.put(mEntityId, FAMILY, QUAL0, 1L, "string1");
    mWriter.put(mEntityId, FAMILY, QUAL0, 2L, "string2");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).addFamily(MAP))
        .build();

    final KijiRowData row1 = mReader.get(mEntityId, dataRequest);
    final Iterator<KijiCell<Integer>> cells = row1.iterator(MAP);

    assertTrue(cells.hasNext());
    final KijiCell<?> cell0 = cells.next();
    assertEquals("Wrong first cell!", KEY0, cell0.getQualifier());

    assertTrue(cells.hasNext());
    final KijiCell<?> cell1 = cells.next();
    assertEquals("Wrong second cell!", KEY1, cell1.getQualifier());

    assertTrue(cells.hasNext());
    final KijiCell<?> cell2 = cells.next();
    assertEquals("Wrong third cell!", KEY2, cell2.getQualifier());
    assertFalse(cells.hasNext());

    final Iterator<KijiCell<Integer>> cellsKey1 = row1.iterator("map", "key1");
    assertTrue(cellsKey1.hasNext());

    final KijiCell<Integer> key1Cell = cellsKey1.next();
    assertEquals("key1", key1Cell.getQualifier());
    assertEquals(1L, key1Cell.getTimestamp());
    assertEquals((Integer) 1, key1Cell.getData());
    assertFalse(cellsKey1.hasNext());
  }

  @Test
  public void testIteratorMapFamilyMaxVersionsTypes() throws IOException {
    mWriter.put(mEntityId, MAP, KEY0, 1L, 0);
    mWriter.put(mEntityId, MAP, KEY0, 2L, 1);
    mWriter.put(mEntityId, MAP, KEY0, 3L, 2);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).addFamily("map"))
        .build();

    final KijiRowData row1 = mReader.get(mEntityId, dataRequest);
    final Iterator<KijiCell<Integer>> cells = row1.iterator(MAP);
    assertTrue(cells.hasNext());

    final KijiCell<Integer> cell0 = cells.next();
    assertEquals("Wrong first cell!", 2, cell0.getData().intValue());
    assertTrue(cells.hasNext());

    final KijiCell<Integer> cell1 = cells.next();
    assertEquals("Wrong second cell!", 1, cell1.getData().intValue());
    assertFalse(cells.hasNext());
  }

  @Test
  public void testMapAsIterable() throws IOException {
    mWriter.put(mEntityId, MAP, KEY0, 1L, 0);
    mWriter.put(mEntityId, MAP, KEY1, 1L, 1);
    mWriter.put(mEntityId, MAP, KEY2, 1L, 2);
    mWriter.put(mEntityId, FAMILY, QUAL0, 1L, "string1");
    mWriter.put(mEntityId, FAMILY, QUAL0, 2L, "string2");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).addFamily("map"))
        .build();

    final KijiRowData row1 = mReader.get(mEntityId, dataRequest);
    final List<KijiCell<Integer>> cells = Lists.newArrayList(row1.<Integer>asIterable(MAP));
    final int cellCount = cells.size();
    assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
  }
}
