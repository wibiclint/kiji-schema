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
import org.kiji.schema.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static org.junit.Assert.*;

/** Simple read/write tests. */
public class TestCassandraReadAndWrite extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraReadAndWrite.class);

  @Test
  public void testBasicReadAndWrite() throws Exception {
    LOG.info("Opening an in-memory kiji instance");
    final Kiji kiji = getKiji();

    LOG.info(String.format("Opened fake Kiji '%s'.", kiji.getURI()));
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));

    final KijiTable table = kiji.openTable("table");
    final KijiTableWriter writer = table.openTableWriter();
    final KijiTableReader reader = table.openTableReader();

    EntityId eid0 = table.getEntityId("row0");

    writer.put(eid0, "family", "column", 0L, "Value at timestamp 0.");
    writer.put(eid0, "family", "column", 1L, "Value at timestamp 1.");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "column"))
        .build();

    // Try this as a get.
    KijiRowData rowData = reader.get(eid0, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");

    // Delete the cell and make sure that this value is missing.
    writer.deleteCell(eid0, "family", "column", 0L);

    rowData = reader.get(eid0, dataRequest);
    assertFalse(rowData.containsCell("family", "column", 0L));

    reader.close();
    writer.close();
    kiji.release();
  }

  /** Try deleting an entire column. */
   @Test
  public void testDeleteColumn() throws Exception {
    final Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE));

    final KijiTable table = kiji.openTable("table");
    final KijiTableWriter writer = table.openTableWriter();
    final KijiTableReader reader = table.openTableReader();

    EntityId eid0 = table.getEntityId("row0");

    writer.put(eid0, "family", "column", 0L, "Value at timestamp 0.");
    writer.put(eid0, "family", "column", 1L, "Value at timestamp 1.");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("family", "column"))
        .build();

    // Try this as a get.
    KijiRowData rowData = reader.get(eid0, dataRequest);
    String s = rowData.getValue("family", "column", 0L).toString();
    assertEquals(s, "Value at timestamp 0.");

    // Delete the entire column.
    writer.deleteColumn(eid0, "family", "column");

    rowData = reader.get(eid0, dataRequest);

    // Should not get any data back!
    assertFalse( rowData.containsColumn("family", "column") );

    reader.close();
    writer.close();
    kiji.release();
  }

  /**
   * Test corner cases for a row scanner.  The row scanner may have to issue multiple discrete
   * queries for a single `KijiDataRequest`.  Each query will return an iterator over Cassandra
   * `Row` objects, which the `KijiRowScanner` will then assemble back into `KijiRowData` objects.
   *
   * Below we test a corner case in which several Kiji rows have data for different column
   * qualifiers.  We structure the data request such that we specifically request data for the
   * different qualifiers, meaning that we will see a different Cassandra query for each qualifier.
   * Thus, the `KijiRowScanner` will get back a different `Row` iterator for each unique qualifier,
   * and then have to assemble them back together.
   */
  @Test
  public void testRowScannerSparseData() throws Exception {
    final Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE_MAP_TYPE));

    final KijiTable table = kiji.openTable("users");
    final KijiTableWriter writer = table.openTableWriter();

    // Reuse these entity IDs for puts and for gets.
    EntityId ALICE = table.getEntityId("Alice");
    EntityId BOB = table.getEntityId("Bob");
    EntityId CATHY = table.getEntityId("Cathy");
    EntityId DAVID = table.getEntityId("David");

    final String PETS = "pets";
    final String CAT = "cat";
    final String DOG = "dog";
    final String RABBIT = "rabbit";
    final String FISH = "fish";
    final String BIRD = "bird";

    // Insert some data into the table.  Give various users different pets.
    writer.put(ALICE, PETS, CAT, 0L, "Alister");
    writer.put(BOB, PETS, CAT, 0L, "Buffy");
    writer.put(CATHY, PETS, CAT, 0L, "Mr Cat");
    writer.put(DAVID, PETS, CAT, 0L, "Dash");

    writer.put(ALICE, PETS, DOG, 0L, "Amour");
    writer.put(BOB, PETS, RABBIT, 0L, "Bounce");
    writer.put(CATHY, PETS, FISH, 0L, "Catfish");
    writer.put(DAVID, PETS, BIRD, 0L, "Da Bird");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(
            ColumnsDef
                .create()
                .add(PETS, CAT)
                .add(PETS, DOG)
                .add(PETS, RABBIT)
                .add(PETS, FISH)
                .add(PETS, BIRD)
        ).build();

    // Fire up a row scanner!
    KijiRowScanner scanner = table.openTableReader().getScanner(dataRequest);

    // There is a small enough amount of data that we can just put all of the rows into a hash from
    // entity ID to row data.
    HashMap<EntityId, KijiRowData> allData = new HashMap<EntityId, KijiRowData>();

    for (KijiRowData row : scanner) {
      EntityId eid = row.getEntityId();
      assert(!allData.containsKey(eid));
      allData.put(eid, row);
    }

    assertTrue(allData.containsKey(ALICE));
    assertTrue(allData.containsKey(BOB));
    assertTrue(allData.containsKey(CATHY));
    assertTrue(allData.containsKey(DAVID));

    assertTrue(allData.get(ALICE).containsColumn(PETS, CAT));
    assertTrue(allData.get(ALICE).containsColumn(PETS, DOG));
    assertFalse(allData.get(ALICE).containsColumn(PETS, RABBIT));
    assertFalse(allData.get(ALICE).containsColumn(PETS, FISH));
    assertFalse(allData.get(ALICE).containsColumn(PETS, BIRD));

    assertTrue(allData.get(BOB).containsColumn(PETS, CAT));
    assertTrue(allData.get(BOB).containsColumn(PETS, RABBIT));
    assertFalse(allData.get(BOB).containsColumn(PETS, DOG));
    assertFalse(allData.get(BOB).containsColumn(PETS, FISH));
    assertFalse(allData.get(BOB).containsColumn(PETS, BIRD));

    assertTrue(allData.get(CATHY).containsColumn(PETS, CAT));
    assertTrue(allData.get(CATHY).containsColumn(PETS, FISH));
    assertFalse(allData.get(CATHY).containsColumn(PETS, DOG));
    assertFalse(allData.get(CATHY).containsColumn(PETS, RABBIT));
    assertFalse(allData.get(CATHY).containsColumn(PETS, BIRD));

    assertTrue(allData.get(DAVID).containsColumn(PETS, CAT));
    assertTrue(allData.get(DAVID).containsColumn(PETS, BIRD));
    assertFalse(allData.get(DAVID).containsColumn(PETS, DOG));
    assertFalse(allData.get(DAVID).containsColumn(PETS, RABBIT));
    assertFalse(allData.get(DAVID).containsColumn(PETS, FISH));

    writer.close();
    kiji.release();
  }
}
