/**
 * (c) Copyright 2013 WibiData, Inc.
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.impl.LayoutCapsule;
import org.kiji.schema.impl.cassandra.CassandraDataRequestAdapter;
import org.kiji.schema.impl.cassandra.CassandraKijiTable;
import org.kiji.schema.impl.cassandra.CassandraVersionPager;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.layout.impl.cassandra.CassandraColumnNameTranslator;
import org.kiji.schema.util.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class TestCassandraVersionPager extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraVersionPager.class);

  private static final int NJOBS = 5;
  private static final long NTIMESTAMPS = 5;

  private KijiTableReader mReader;
  private static KijiTable mTable;

  @BeforeClass
  public static void initShared() {
    CassandraKijiClientTest clientTest = new CassandraKijiClientTest();
    try {
      clientTest.setupKijiTest();
      Kiji kiji = clientTest.getKiji();
      kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.PAGING_TEST));

      mTable = kiji.openTable("user");
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
    } catch (Exception e) {
      throw new KijiIOException(e);
    }
  }

  @Before
  public final void setupTestKijiPager() throws Exception {
    mReader = mTable.openTableReader();
  }

  @After
  public final void tearDownTestKijiPager() throws IOException {
    mReader.close();
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    mTable.release();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testColumnPagingNotEnabled() throws IOException {
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(5).add("info", "name"))
        .build();
    assertTrue(!dataRequest.isEmpty());
    assertTrue(!dataRequest.isPagingEnabled());
    final EntityId meId = mTable.getEntityId(Bytes.toBytes("me"));
    final KijiRowData myRowData = mReader.get(meId, dataRequest);
    try {
      myRowData.getPager("info", "name");
      Assert.fail("Paging is not enabled!");
    } catch (KijiColumnPagingNotEnabledException kcpnee) {
      // Expected!
    }
  }

  /** Test that a pager retrieved for a group type column family acts as expected. */
  @Test
  public void testVersionsPager() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final int maxVersions = 5;  // == actual number of versions in the column

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(maxVersions).withPageSize(2).add("info", "name"))
        .build();
    assertTrue(!dataRequest.isEmpty());
    assertTrue(dataRequest.isPagingEnabled());
    assertTrue(dataRequest.getColumn("info", "name").isPagingEnabled());

    final KijiRowData row = mReader.get(eid, dataRequest);
    final KijiPager pager = row.getPager("info", "name");
    assertTrue(pager instanceof CassandraVersionPager);

    try {
      assertTrue(pager.hasNext());

      final List<KijiCell<CharSequence>> cells = Lists.newArrayList();
      final List<Integer> pageSizes = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        final KijiRowData page = pager.next();
        assertNotNull(page);
        Iterable<KijiCell<CharSequence>> iterable = page.<CharSequence>asIterable("info", "name");
        assertNotNull(iterable);
        final List<KijiCell<CharSequence>> pageCells = Lists.newArrayList(iterable);
        cells.addAll(pageCells);
        LOG.info("Page #{}: {}", npage, pageCells);
        npage += 1;
        pageSizes.add(pageCells.size());
      }

      assertFalse(pager.hasNext());
      try {
        pager.next();
        fail();
      } catch (NoSuchElementException nsee) {
        // Expected!
      }

      assertEquals(Lists.newArrayList(2, 2, 1), pageSizes);
      assertEquals(maxVersions, cells.size());
      int counter = 5;
      for (KijiCell<CharSequence> cell : cells) {
        assertEquals(counter, cell.getTimestamp());
        counter -= 1;
      }

    } finally {
      pager.close();
    }
  }

  /** Test case where max-versions is less than the actual number of versions in the column. */
  @Test
  //public void testVersionsPagerMaxVersionsLessThanActual() throws IOException {
  public void testMaxVersionsLessThanActual() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final int maxVersions = 3;  // < actual number of versions in the column (which is 5)

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(maxVersions).withPageSize(2).add("info", "name"))
        .build();
    assertTrue(!dataRequest.isEmpty());
    assertTrue(dataRequest.isPagingEnabled());
    assertTrue(dataRequest.getColumn("info", "name").isPagingEnabled());

    final KijiRowData row = mReader.get(eid, dataRequest);
    final KijiPager pager = row.getPager("info", "name");
    assertTrue(pager instanceof CassandraVersionPager);

    try {
      assertTrue(pager.hasNext());

      final List<KijiCell<CharSequence>> cells = Lists.newArrayList();
      final List<Integer> pageSizes = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        final KijiRowData page = pager.next();
        final List<KijiCell<CharSequence>> pageCells =
            Lists.newArrayList(page.<CharSequence>asIterable("info", "name"));
        cells.addAll(pageCells);
        LOG.info("Page #{}: {}", npage, pageCells);
        npage += 1;
        pageSizes.add(pageCells.size());
      }

      assertFalse(pager.hasNext());
      try {
        pager.next();
        fail();
      } catch (NoSuchElementException nsee) {
        // Expected!
      }

      assertEquals(Lists.newArrayList(2, 1), pageSizes);
      assertEquals(maxVersions, cells.size());
      int counter = 5;
      for (KijiCell<CharSequence> cell : cells) {
        assertEquals(counter, cell.getTimestamp());
        counter -= 1;
      }

    } finally {
      pager.close();
    }
  }

  /** Test case where max-versions is greater than the actual number of versions in the column. */
  @Test
  //public void testVersionsPagerMaxVersionsGreaterThanActual() throws IOException {
  public void testMaxVersionsGTActual() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final int maxVersions = 7;     // > actual number of versions in the column (which is 5)
    final int actualVersions = 5;

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(maxVersions).withPageSize(2).add("info", "name"))
        .build();
    assertTrue(!dataRequest.isEmpty());
    assertTrue(dataRequest.isPagingEnabled());
    assertTrue(dataRequest.getColumn("info", "name").isPagingEnabled());

    final KijiRowData row = mReader.get(eid, dataRequest);
    final KijiPager pager = row.getPager("info", "name");
    assertTrue(pager instanceof CassandraVersionPager);

    try {
      assertTrue(pager.hasNext());

      final List<KijiCell<CharSequence>> cells = Lists.newArrayList();
      final List<Integer> pageSizes = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        final KijiRowData page = pager.next();
        final List<KijiCell<CharSequence>> pageCells =
            Lists.newArrayList(page.<CharSequence>asIterable("info", "name"));
        cells.addAll(pageCells);
        LOG.info("Page #{}: {}", npage, pageCells);
        npage += 1;
        pageSizes.add(pageCells.size());
      }

      assertEquals(Lists.newArrayList(2, 2, 1), pageSizes);
      assertFalse(pager.hasNext());
      try {
        pager.next();
        fail();
      } catch (NoSuchElementException nsee) {
        // Expected!
      }

      assertEquals(Lists.newArrayList(2, 2, 1), pageSizes);
      assertEquals(actualVersions, cells.size());
      int counter = 5;
      for (KijiCell<CharSequence> cell : cells) {
        assertEquals(counter, cell.getTimestamp());
        counter -= 1;
      }

    } finally {
      pager.close();
    }
  }

  /** Test that a pager retrieved for a group type column family acts as expected. */
  @Test
  public void testGroupTypeColumnPagingFromScan() throws IOException {
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(5).withPageSize(2).add("info", "name"))
        .build();
    assertTrue(!dataRequest.isEmpty());
    assertTrue(dataRequest.isPagingEnabled());
    assertTrue(dataRequest.getColumn("info", "name").isPagingEnabled());

    final KijiRowScanner scanner = mReader.getScanner(dataRequest);
    try {
      final Iterator<KijiRowData> iterator = scanner.iterator();
      assertTrue(iterator.hasNext());
      final KijiRowData myRowData = iterator.next();
      final KijiPager pager = myRowData.getPager("info", "name");
      assertTrue(pager.hasNext());

      final NavigableMap<Long, CharSequence> resultMap = pager.next().getValues("info", "name");
      assertEquals("The number of returned values is incorrect: ", 2, resultMap.size());
      assertEquals("Incorrect first value of first page:", "me-five", resultMap.get(5L).toString());
      assertEquals("Incorrect second value of first page:", "me-four",
          resultMap.get(4L).toString());
      assertTrue(pager.hasNext());
      final NavigableMap<Long, CharSequence> resultMap2 = pager.next().getValues("info", "name");
      assertEquals("The number of returned values is incorrect: ", 2 , resultMap2.size());
      assertEquals("Incorrect first value of second page:", "me-three",
          resultMap2.get(3L).toString());
      assertEquals("Incorrect second value of second page:", "me-two",
          resultMap2.get(2L).toString());

      assertTrue(pager.hasNext());
      final NavigableMap<Long, CharSequence> resultMap3 = pager.next().getValues("info", "name");
      assertEquals("The number of returned values is incorrect: ", 1 , resultMap3.size());
      assertEquals(
          "Incorrect first value of second page:", "me-one", resultMap3.get(1L).toString());
      ResourceUtils.closeOrLog(pager);

      assertTrue(!iterator.hasNext());
    } finally {
      scanner.close();
    }
  }

  /** Test that a pager retrieved for a group type column family acts as expected. */
  @Test
  public void testGroupMaxVersions() throws IOException {
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(3).withPageSize(2).add("info", "name"))
        .build();

    assertTrue(!dataRequest.isEmpty());
    assertTrue(dataRequest.isPagingEnabled());
    assertTrue(dataRequest.getColumn("info", "name").isPagingEnabled());
    EntityId meId = mTable.getEntityId(Bytes.toBytes("me"));
    KijiRowData myRowData = mReader.get(meId, dataRequest);
    KijiPager pager = myRowData.getPager("info", "name");
    try {
      assertTrue(pager.hasNext());

      final NavigableMap<Long, CharSequence> resultMap = pager.next().getValues("info", "name");
      assertEquals("The number of returned values is incorrect: ", 2, resultMap.size());
      assertEquals("Incorrect first value of first page:", "me-five", resultMap.get(5L).toString());
      assertEquals(
          "Incorrect second value of first page:", "me-four", resultMap.get(4L).toString());
      assertTrue(pager.hasNext());
      final NavigableMap<Long, CharSequence> resultMap2 = pager.next().getValues("info", "name");
      assertEquals("The number of returned values is incorrect: ", 1 , resultMap2.size());
      assertEquals("Incorrect first value of second page:", "me-three",
          resultMap2.get(3L).toString());
      assertFalse(pager.hasNext());
    } finally {
      pager.close();
    }
  }

  /** Test the version pager on a fully-qualified column from a map-type family. */
  @Test
  public void testVersionPagerOnMapTypeFamily() throws Exception {
    final int nversions = 5;
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(nversions)
            .withPageSize(2)
            .addFamily("jobs"))
        .build();
    final EntityId eid = mTable.getEntityId(Bytes.toBytes("me"));
    final KijiRowData row = mReader.get(eid, dataRequest);
    for (String qualifier : ImmutableList.of("j1", "j3")) {
      LOG.info("Testing with qualifier: {}", qualifier);
      final KijiPager pager = row.getPager("jobs", qualifier);
      try {
        final List<String> titles = Lists.newArrayList();
        int npages = 0;
        while (pager.hasNext()) {
          final KijiRowData page = pager.next();
          titles.addAll(page.<String>getValues("jobs", qualifier).values());
          npages += 1;
        }
        // 5 versions with a page size of 2 implies at least 3 pages from the pager:
        //     [2 versions, 2 versions, 1 version].
        assertTrue(npages >= 3);
        assertEquals(nversions, titles.size());
      } finally {
        pager.close();
      }
    }
  }

}
