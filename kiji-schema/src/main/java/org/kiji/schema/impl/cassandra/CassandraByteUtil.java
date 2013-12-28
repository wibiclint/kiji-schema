package org.kiji.schema.impl.cassandra;

import java.nio.ByteBuffer;

/**
 * Useful static classes for converting between ByteBuffers and byte arrays.
 */
public class CassandraByteUtil {
  public static byte[] byteBuffertoBytes(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    return bytes;
  }

  public static ByteBuffer bytesToByteBuffer(byte[] bytes) {
    return ByteBuffer.wrap(bytes);
  }
}
