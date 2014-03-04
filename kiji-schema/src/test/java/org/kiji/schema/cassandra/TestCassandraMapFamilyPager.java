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

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.junit.*;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.filter.KijiColumnRangeFilter;
import org.kiji.schema.impl.cassandra.CassandraKijiRowData;
import org.kiji.schema.layout.KijiTableLayouts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestCassandraMapFamilyPager extends CassandraKijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestCassandraMapFamilyPager.class);

  private static KijiTable mTable;
  private KijiTableReader mReader;
  private EntityId mEntityId;
  private static final int NJOBS = 5;
  private static final long NTIMESTAMPS = 5;

  /** Use to create unique entity IDs for each test case. */
  private static AtomicInteger testIdCounter;


  @BeforeClass
  public static void setupTestCassandraQualifierPager() throws Exception {
    CassandraKijiClientTest clientTest = new CassandraKijiClientTest();
    clientTest.setupKijiTest();

    Kiji kiji = clientTest.getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.PAGING_TEST));

    mTable = kiji.openTable("user");
    testIdCounter = new AtomicInteger(0);
  }

  @Before
  public final void setupEnvironment() throws Exception {
    // Fill local variables.
    mReader = mTable.openTableReader();
    mEntityId = mTable.getEntityId("eid-" + testIdCounter.getAndIncrement());

    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      for (int job = 0; job < NJOBS; ++job) {
        for (long ts = 1; ts <= NTIMESTAMPS; ++ts) {
          writer.put(mEntityId, "jobs", String.format("j%d", job), ts, String.format("j%d-t%d", job, ts));
        }
      }
    } finally {
      writer.close();
    }
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mReader.close();
  }

  @AfterClass
  public static void cleanupClass() throws IOException {
    mTable.release();
  }

  /** Test a qualifier pager on a map-type family with no user filter. */
  @Test
  public void testQualifiersPager() throws IOException {
    final int pageSize = 2;
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(HConstants.ALL_VERSIONS).withPageSize(pageSize).addFamily("jobs"))
        .build();

    final KijiRowData row = mReader.get(mEntityId, dataRequest);
    assert(row instanceof CassandraKijiRowData);
    final KijiPager pager = row.getPager("jobs");
    assertNotNull(pager);
    try {
      assertTrue(pager.hasNext());

      final List<String> qualifiers = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        // Note: we cannot validate individual pages as their boundaries depend on HBase version.
        final NavigableSet<String> page = pager.next().getQualifiers("jobs");
        LOG.info("Page #{}: {}", npage, page);
        assertTrue(page.size() <= pageSize);
        qualifiers.addAll(page);
        npage++;
      }

      final List<String> expected = Lists.newArrayList("j0", "j1", "j2", "j3", "j4");
      assertTrue(npage >= 3);  // at least 3 pages
      assertEquals(expected, qualifiers);

      assertFalse(pager.hasNext());
      try {
        pager.next();
        Assert.fail("next() should throw NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        // Expected
      }
    } finally {
      pager.close();
    }
  }

  /** Test a qualifier pager on a map-type family with a user filter that discards everything. */
  @Test
  public void testQualifiersPagerWithUserFilterEmpty() throws IOException {
    final int pageSize = 2;
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(pageSize)
            .withFilter(new KijiColumnRangeFilter("j12", true, "j13", true))
            .addFamily("jobs"))
        .build();

    final KijiRowData row = mReader.get(mEntityId, dataRequest);
    final KijiPager pager = row.getPager("jobs");
    assertNotNull(pager);
    try {
      assertTrue(pager.hasNext());
      assertTrue(pager.next().getQualifiers("jobs").isEmpty());
      assertFalse(pager.hasNext());
      try {
        pager.next();
        Assert.fail("next() should throw NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        // Expected
      }
    } finally {
      pager.close();
    }
  }

  /** Test a qualifier pager on a map-type family with a user filter. */
  @Test
  public void testQualifiersPagerWithUserFilter() throws IOException {
    final int pageSize = 2;
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(pageSize)
            .withFilter(new KijiColumnRangeFilter("j1", true, "j2", true))
            .addFamily("jobs"))
        .build();

    final KijiRowData row = mReader.get(mEntityId, dataRequest);
    final KijiPager pager = row.getPager("jobs");
    assertNotNull(pager);
    try {
      assertTrue(pager.hasNext());

      final List<String> qualifiers = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        // Note: we cannot validate individual pages as their boundaries depend on HBase version.
        final NavigableSet<String> page = pager.next().getQualifiers("jobs");
        LOG.info("Page #{}: {}", npage, page);
        assertTrue(page.size() <= pageSize);
        qualifiers.addAll(page);
        npage++;
      }

      final List<String> expected = Lists.newArrayList("j1", "j2");
      assertTrue(npage >= 1);  // at least 1 page
      assertEquals(expected, qualifiers);

      assertFalse(pager.hasNext());
      try {
        pager.next();
        Assert.fail("next() should throw NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        // Expected
      }
    } finally {
      pager.close();
    }
  }

  /** Test a qualifier pager on a map-type family with a user filter and several pages. */
  @Test
  public void testQualifiersPagerWithUserFilter2() throws IOException {
    final int pageSize = 2;
    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(pageSize)
            .withFilter(new KijiColumnRangeFilter("j1", true, null, true))
            .addFamily("jobs"))
        .build();

    final KijiRowData row = mReader.get(mEntityId, dataRequest);
    final KijiPager pager = row.getPager("jobs");
    assertNotNull(pager);
    try {
      assertTrue(pager.hasNext());

      final List<String> qualifiers = Lists.newArrayList();
      int npage = 0;
      while (pager.hasNext()) {
        // Note: we cannot validate individual pages as their boundaries depend on HBase version.
        final NavigableSet<String> page = pager.next().getQualifiers("jobs");
        LOG.info("Page #{}: {}", npage, page);
        assertTrue(page.size() <= pageSize);
        qualifiers.addAll(page);
        npage++;
      }

      final List<String> expected = Lists.newArrayList("j1", "j2", "j3", "j4");
      assertTrue(npage >= 2);  // at least 2 pages
      assertEquals(expected, qualifiers);

      assertFalse(pager.hasNext());
      try {
        pager.next();
        Assert.fail("next() should throw NoSuchElementException");
      } catch (NoSuchElementException nsee) {
        // Expected
      }
    } finally {
      pager.close();
    }
  }

  /** Test illustrating how paging through all cells in a map-type family works. */
  @Test
  public void testFullMapPaging() throws Exception {
    final int qualifiersPageSize = 3;
    final int versionsPageSize = 3;

    final KijiDataRequest qualifiersDataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withPageSize(qualifiersPageSize)
            .addFamily("jobs"))
        .build();

    final KijiRowData qualifiersRow = mReader.get(mEntityId, qualifiersDataRequest);
    final KijiPager qualifiersPager = qualifiersRow.getPager("jobs");
    try {
      int qualifiersCounter = 0;

      while (qualifiersPager.hasNext()) {
        final KijiRowData qualifierPage = qualifiersPager.next();
        LOG.debug("New qualifier page with {} qualifiers",
            qualifierPage.getQualifiers("jobs").size());
        for (String qualifier : qualifierPage.getQualifiers("jobs")) {
          qualifiersCounter += 1;
          int versionsCounter = 0;

          final KijiDataRequest versionsDataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withPageSize(versionsPageSize)
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("jobs", qualifier))
              .build();
          final KijiRowData versionsRow = mReader.get(mEntityId, versionsDataRequest);
          final KijiPager versionsPager = versionsRow.getPager("jobs", qualifier);
          while (versionsPager.hasNext()) {
            final KijiRowData versionsPage = versionsPager.next();
            LOG.debug("New version page with {} versions",
                versionsPage.getValues("jobs", qualifier).size());
            for (Map.Entry<Long, String> entry
                : versionsPage.<String>getValues("jobs", qualifier).entrySet()) {
              versionsCounter += 1;
              LOG.debug("Entry: {} -> {}", entry.getKey(), entry.getValue());
            }
          }

          assertEquals(NTIMESTAMPS, versionsCounter);
        }
      }

      assertEquals(NJOBS, qualifiersCounter);
    } finally {
      qualifiersPager.close();
    }
  }
}
