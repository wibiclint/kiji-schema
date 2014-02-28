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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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

  private final static int KEY0_VAL = 100;
  private final static int KEY1_VAL = 101;

  private EntityIdFactory mEntityIdFactory;

  /** KijiTable used for some tests (named TABLE_NAME). */
  private CassandraKijiTable mTable;

  private static final Node mNode0 = Node.newBuilder().setLabel("node0").build();
  private static final Node mNode1 = Node.newBuilder().setLabel("node1").build();

  @Before
  public final void setupTestHBaseKijiRowData() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1));
    mTable = CassandraKijiTable.downcast(getKiji().openTable(TABLE_NAME));

    final LayoutCapsule capsule = mTable.getLayoutCapsule();
    final ColumnNameTranslator translator = capsule.getColumnNameTranslator();

    mEntityIdFactory = EntityIdFactory.getFactory(capsule.getLayout());
  }

  @After
  public final void teardownTestHBaseKijiRowData() throws Exception {
    mTable.release();
    mTable = null;
  }

  // -----------------------------------------------------------------------------------------------
  // Test cases that need to interact with an actual Kiji table.

  @Test
  public void testEntityId() throws Exception {
    final List<KeyValue> kvs = Lists.newArrayList();
    final EntityId foo = mEntityIdFactory.getEntityId("foo");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add(FAMILY, QUAL0))
        .build();

    // Put some data into the table.
    KijiTableWriter writer = mTable.getWriterFactory().openTableWriter();
    writer.put(foo, FAMILY, QUAL0, Bytes.toBytes("bot"));
    writer.close();

    // Read out the results to get a KijiRowData
    KijiTableReader reader = mTable.getReaderFactory().openTableReader();
    final KijiRowData input = reader.get(foo, dataRequest);
    assertEquals(foo, input.getEntityId());
  }

  @Test
  public void testReadInts() throws Exception {
    final EntityId row0 = mEntityIdFactory.getEntityId("row0");

    // Put some data into the table.
    KijiTableWriter writer = mTable.getWriterFactory().openTableWriter();
    writer.put(row0, FAMILY, QUAL3, 1L, 42);
    writer.close();

    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    //builder.newColumnsDef().addFamily(FAMILY);
    builder.newColumnsDef().add(FAMILY, QUAL3);
    KijiDataRequest dataRequest = builder.build();

    // Read out the results to get a KijiRowData
    KijiTableReader reader = mTable.getReaderFactory().openTableReader();
    final KijiRowData input = reader.get(row0, dataRequest);

    assertNotNull(input.getMostRecentValue(FAMILY, QUAL3));
    final int integer = (Integer) input.getMostRecentValue(FAMILY, QUAL3);
    assertEquals(42, integer);
  }

  @Test
  public void testGetReaderSchema() throws Exception {
    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();

    // Read data for an entity ID that does not exist.
    KijiTableReader reader = mTable.getReaderFactory().openTableReader();
    final KijiRowData input = reader.get(mEntityIdFactory.getEntityId("hmm"), dataRequest);

    assertEquals(Schema.create(Schema.Type.STRING), input.getReaderSchema("family", "empty"));
    assertEquals(Schema.create(Schema.Type.INT), input.getReaderSchema("family", "qual3"));
  }

  @Test
  public void testGetReaderSchemaNoSuchColumn() throws Exception {
    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();

    // Read data for an entity ID that does not exist.
    KijiTableReader reader = mTable.getReaderFactory().openTableReader();
    final KijiRowData input = reader.get(mEntityIdFactory.getEntityId("hmm"), dataRequest);

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

  // Tests for KijiRowData.getReaderSchema() with layout-1.3 tables.
  @Test
  public void testGetReaderSchemaLayout13() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1_3))
        .build();
    final KijiTable table = kiji.openTable("table");
    try {
      final KijiTableReader reader = table.getReaderFactory().openTableReader();
      try {
        final EntityId eid = table.getEntityId("row");
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().addFamily("family"))
            .build();
        final KijiRowData row = reader.get(eid, dataRequest);
        assertEquals(
            Schema.Type.STRING,
            row.getReaderSchema("family", "qual0").getType());

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
  @Test
  public void testReadMiddleTimestamp() throws IOException {
    // Test that we can select a timestamped value that is not the most recent value.
    new InstanceBuilder(getKiji())
        .withTable(mTable)
        .withRow("row1")
        .withFamily("family")
        .withQualifier("qual0")
        .withValue(4L, "oldest")
        .withValue(6L, "middle")
        .withValue(8L, "newest")
        .withQualifier("qual1")
        .withValue(1L, "one")
        .withValue(2L, "two")
        .withValue(3L, "three")
        .withValue(4L, "four")
        .withValue(8L, "eight")
        .withQualifier("qual2")
        .withValue(3L, "q2-three")
        .withValue(4L, "q2-four")
        .withValue(6L, "q2-six")
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .withTimeRange(2L, 7L)
        .addColumns(ColumnsDef.create().add("family", "qual0"))
        .addColumns(ColumnsDef.create().withMaxVersions(2).add("family", "qual1"))
        .addColumns(ColumnsDef.create().withMaxVersions(3).add("family", "qual2"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);

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
    } finally {
      reader.close();
    }
  }

  @Test
  public void testEmptyResult() throws IOException {
    // TODO: Test having results for a family, but not for a particular qualifier.
    // TODO: Test not having results for family or qualifier.
    new InstanceBuilder(getKiji())
        .withTable(mTable)
        .withRow("row1")
        .withFamily("family")
        .withQualifier("qual0").withValue(1L, "string1")
        .withQualifier("qual0").withValue(2L, "string2")
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "qual1"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);

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

    } finally {
      reader.close();
    }
  }

  // Tests that reading an entire family with a column that has been deleted works.
  @Test
  public void testReadDeletedColumns() throws Exception {
    final Kiji kiji = getKiji();
    new InstanceBuilder(kiji)
        .withTable(mTable)
        .withRow("row1")
        .withFamily("family")
        .withQualifier("qual0").withValue(1L, "string1")
        .withQualifier("qual0").withValue(2L, "string2")
        .build();

    final TableLayoutDesc update = KijiTableLayouts.getLayout(TEST_LAYOUT_V2);
    update.setReferenceLayout(mTable.getLayout().getDesc().getLayoutId());
    kiji.modifyTableLayout(update);

    mTable.release();
    mTable = (CassandraKijiTable) kiji.openTable(TABLE_NAME);

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().addFamily("family"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);
      assertTrue(row1.getValues("family", "qual0").isEmpty());

    } finally {
      reader.close();
    }
  }

  //    Tests that we can read a record using the writer schema.
  //    This tests the case when a specific record class is not found on the classpath.
  //    However, this behavior is bogus. The reader schema should not be tied to the classes
  //    available on the classpath.
  //
  //    TODO(SCHEMA-295) the user may force using the writer schemas by overriding the
  //        declared reader schemas. This test will be updated accordingly.
  @Test
  public void testWSchemaWhenSpecRecClassNF() throws Exception {
    final Kiji kiji = getKiji();  // not owned
    kiji.createTable(KijiTableLayouts.getLayout(WRITER_SCHEMA_TEST));
    final KijiTable table = kiji.openTable("writer_schema");
    try {
      // Write a (generic) record:
      final Schema writerSchema = Schema.createRecord("Found", null, "class.not", false);
      writerSchema.setFields(Lists.newArrayList(
          new Field("field", Schema.create(Schema.Type.STRING), null, null)));

      final KijiTableWriter writer = table.openTableWriter();
      try {
        final GenericData.Record record = new GenericRecordBuilder(writerSchema)
            .set("field", "value")
            .build();
        writer.put(table.getEntityId("eid"), "family", "qualifier", 1L, record);

      } finally {
        writer.close();
      }

      // Read the record back (should be a generic record):
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiDataRequest dataRequest = KijiDataRequest.builder()
            .addColumns(ColumnsDef.create().add("family", "qualifier"))
            .build();
        final KijiRowData row = reader.get(table.getEntityId("eid"), dataRequest);
        final GenericData.Record record = row.getValue("family", "qualifier", 1L);
        assertEquals(writerSchema, record.getSchema());
        assertEquals("value", record.get("field").toString());
      } finally {
        reader.close();
      }

    } finally {
      table.release();
    }
  }

  // This test was created in response to WIBI-41.  If your KijiDataRequest doesn't contain
  // one of the columns in the Result map, you used to a get a NullPointerException.
  @Test
  public void testGetMap() throws Exception {
    final EntityId eid = mEntityIdFactory.getEntityId("foo");

    // Put some data into the table.
    KijiTableWriter writer = mTable.getWriterFactory().openTableWriter();
    writer.put(eid, FAMILY, QUAL0, Bytes.toBytes("bot"));
    writer.put(eid, FAMILY, EMPTY, Bytes.toBytes("car"));
    writer.close();

    final KijiDataRequest dataRequest = KijiDataRequest.builder().build();
    KijiTableReader reader = mTable.getReaderFactory().openTableReader();
    // We didn't request any data, so the map should be null.
    final KijiRowData input = reader.get(mEntityIdFactory.getEntityId("hmm"), dataRequest);
    assertTrue(((CassandraKijiRowData)input).getMap().isEmpty());
  }

  @Test
  public void testContainsColumn() throws Exception {
    final long timestamp = 1L;
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(TEST_LAYOUT_V1))
        .withRow("row1")
        .withFamily(FAMILY).withQualifier(QUAL0).withValue(timestamp, "foo1")
        .build();
    final KijiTable table = kiji.openTable(TABLE_NAME);
    try {
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiRowData row1 = reader.get(table.getEntityId("row1"),
            KijiDataRequest.create(FAMILY, QUAL0));
        assertTrue(row1.containsCell(FAMILY, QUAL0, timestamp));
        assertFalse(row1.containsCell(FAMILY, QUAL0, 2L));
        assertFalse(row1.containsCell("blope", QUAL0, timestamp));
        assertFalse(row1.containsCell(FAMILY, "blope", timestamp));
      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  @Test
  public void testIteratorMapFamilyTypes() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(mTable)
        .withRow("row1")
        .withFamily("map")
        .withQualifier("key0").withValue(1L, 0)
        .withQualifier("key1").withValue(1L, 1)
        .withQualifier("key2").withValue(1L, 2)
        .withFamily("family")
        .withQualifier("qual0").withValue(1L, "string1")
        .withQualifier("qual0").withValue(2L, "string2")
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(1).addFamily("map"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);
      final Iterator<KijiCell<Integer>> cells = row1.iterator("map");
      assertTrue(cells.hasNext());
      final KijiCell<?> cell0 = cells.next();
      assertEquals("Wrong first cell!", "key0", cell0.getQualifier());
      assertTrue(cells.hasNext());
      final KijiCell<?> cell1 = cells.next();
      assertEquals("Wrong second cell!", "key1", cell1.getQualifier());
      assertTrue(cells.hasNext());
      final KijiCell<?> cell2 = cells.next();
      assertEquals("Wrong third cell!", "key2", cell2.getQualifier());
      assertFalse(cells.hasNext());

      final Iterator<KijiCell<Integer>> cellsKey1 = row1.iterator("map", "key1");
      assertTrue(cellsKey1.hasNext());
      final KijiCell<Integer> key1Cell = cellsKey1.next();
      assertEquals("key1", key1Cell.getQualifier());
      assertEquals(1L, key1Cell.getTimestamp());
      assertEquals((Integer) 1, key1Cell.getData());
      assertFalse(cellsKey1.hasNext());

    } finally {
      reader.close();
    }
  }
  @Test
  public void testIteratorMapFamilyMaxVersionsTypes() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(mTable)
        .withRow("row1")
        .withFamily("map")
        .withQualifier("key0")
        .withValue(1L, 0)
        .withValue(2L, 1)
        .withValue(3L, 2)
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(2).addFamily("map"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);
      final Iterator<KijiCell<Integer>> cells = row1.iterator("map");
      assertTrue(cells.hasNext());
      final KijiCell<Integer> cell0 = cells.next();
      assertEquals("Wrong first cell!", 2, cell0.getData().intValue());
      assertTrue(cells.hasNext());
      final KijiCell<Integer> cell1 = cells.next();
      assertEquals("Wrong second cell!", 1, cell1.getData().intValue());
      assertFalse(cells.hasNext());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMapAsIterable() throws IOException {
    new InstanceBuilder(getKiji())
        .withTable(mTable)
        .withRow("row1")
        .withFamily("map")
        .withQualifier("key0").withValue(1L, 0)
        .withQualifier("key1").withValue(1L, 1)
        .withQualifier("key2").withValue(1L, 2)
        .withFamily("family")
        .withQualifier("qual0").withValue(1L, "string1")
        .withQualifier("qual0").withValue(2L, "string2")
        .build();

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).addFamily("map"))
        .build();

    final KijiTableReader reader = mTable.openTableReader();
    try {
      final KijiRowData row1 = reader.get(mTable.getEntityId("row1"), dataRequest);
      final List<KijiCell<Integer>> cells = Lists.newArrayList(row1.<Integer>asIterable("map"));
      final int cellCount = cells.size();
      assertEquals("Wrong number of cells returned by asIterable.", 3, cellCount);
    } finally {
      reader.close();
    }
  }
}
