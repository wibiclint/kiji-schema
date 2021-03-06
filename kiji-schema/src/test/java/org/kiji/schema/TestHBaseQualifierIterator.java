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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.impl.hbase.HBaseKijiTable;
import org.kiji.schema.impl.hbase.HBaseQualifierIterator;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestHBaseQualifierIterator extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseQualifierIterator.class);

  private KijiTableReader mReader;
  private KijiTable mTable;

  private static final int NJOBS = 5;
  private static final long NTIMESTAMPS = 3;

  @Before
  public final void setupTestKijiPager() throws Exception {
    final Kiji kiji = getKiji();
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.PAGING_TEST));

    mTable = kiji.openTable("user");
    final EntityId eid = mTable.getEntityId("me");
    final KijiTableWriter writer = mTable.openTableWriter();
    try {
      for (int job = 0; job < NJOBS; ++job) {
        final long ts = job + 1;
        writer.put(eid, "jobs", String.format("j%d", job), ts, String.format("j%d-t%d", job, ts));
      }
    } finally {
      writer.close();
    }

    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestKijiPager() throws IOException {
    mReader.close();
    mTable.release();
  }

  /** Test a qualifier iterator. */
  @Test
  public void testQualifiersIterator() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(HConstants.ALL_VERSIONS).withPageSize(2).addFamily("jobs"))
        .build();

    final HBaseQualifierIterator iterator =
        new HBaseQualifierIterator(
            eid, dataRequest, (HBaseKijiTable) mTable, new KijiColumnName("jobs"));
    try {
      final List<String> qualifiers = Lists.newArrayList((Iterator<String>) iterator);
      assertEquals(ImmutableList.of("j0", "j1", "j2", "j3", "j4"), qualifiers);
    } finally {
      iterator.close();
    }
  }

  /** Test a qualifier iterator with min/max timestamp restriction. */
  @Test
  public void testQualifiersIteratorMinMaxTimestamp() throws IOException {
    final EntityId eid = mTable.getEntityId("me");

    final KijiDataRequest dataRequest = KijiDataRequest.builder()
        .withTimeRange(2, 4)
        .addColumns(ColumnsDef.create()
            .withMaxVersions(HConstants.ALL_VERSIONS).withPageSize(2).addFamily("jobs"))
        .build();

    final HBaseQualifierIterator iterator =
        new HBaseQualifierIterator(
            eid, dataRequest, (HBaseKijiTable) mTable, new KijiColumnName("jobs"));
    try {
      final List<String> qualifiers = Lists.newArrayList((Iterator<String>) iterator);
      assertEquals(ImmutableList.of("j1", "j2"), qualifiers);
    } finally {
      iterator.close();
    }
  }
}
