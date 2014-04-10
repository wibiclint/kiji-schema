package org.kiji.schema;

import org.junit.Test;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestScratch {

  @Test
  public void  testFoo() throws Exception {
    TableLayoutDesc layout = KijiTableLayouts.getLayout(KijiTableLayouts.READER_SCHEMA_TEST);
    System.out.println(layout);
    System.out.println(layout.getKeysFormat().getClass());
  }
}
