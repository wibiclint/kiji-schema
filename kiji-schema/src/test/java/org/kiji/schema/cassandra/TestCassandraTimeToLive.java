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
import org.kiji.schema.layout.KijiTableLayouts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/** Simple read/write tests. */
public class TestCassandraTimeToLive extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraTimeToLive.class);

  private static KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiTableReader mReader;
  private EntityId mEntityId;

  // This is the family in this table layout with
  private final static String FAMILY = "info";
  private final static String QUALIFIER = "name";
  private final static String VALUE = "Mr Bonkers";
  private final static Long TIMESTAMP = 0L;

  /** Use to create unique entity IDs for each test case. */
  private static AtomicInteger testIdCounter;

  @BeforeClass
  public static void initShared() {
    CassandraKijiClientTest clientTest = new CassandraKijiClientTest();
    testIdCounter = new AtomicInteger(0);
    try {
      clientTest.setupKijiTest();
      Kiji kiji = clientTest.getKiji();
      kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.TTL_TEST));
      mTable = kiji.openTable("ttl_test");
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
  public final void cleanupEnvironment() throws IOException {
    mReader.close();
    mWriter.close();
  }

  @AfterClass
  public static void cleanupClass() throws IOException {
    mTable.release();
  }

  @Test
  public void testTimeToLive() throws Exception {

    // TTL is 10 seconds for this cell.
    mWriter.put(mEntityId, FAMILY, QUALIFIER, TIMESTAMP, VALUE);

    final KijiDataRequest dataRequest = KijiDataRequest.create(FAMILY, QUALIFIER);

    // The data should be there now!
    KijiRowData rowData = mReader.get(mEntityId, dataRequest);
    String s = rowData.getValue(FAMILY, QUALIFIER, TIMESTAMP).toString();
    assertEquals(s, VALUE);

    // Wait for ten seconds.
    Thread.sleep(10*1000);

    rowData = mReader.get(mEntityId, dataRequest);
    assertFalse(rowData.containsCell(FAMILY, QUALIFIER, TIMESTAMP));
  }

}

