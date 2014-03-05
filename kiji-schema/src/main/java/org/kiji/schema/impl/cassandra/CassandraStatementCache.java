package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.PreparedStatement;

import java.util.HashMap;
import java.util.Map;

public class CassandraStatementCache {
  private final CassandraAdmin mAdmin;

  private final Map<String, PreparedStatement> mStatementCache;

  CassandraStatementCache(CassandraAdmin admin) {
    mAdmin = admin;
    mStatementCache = new HashMap<String, PreparedStatement>();
  }

  synchronized PreparedStatement getPreparedStatement(String query) {
    if (!mStatementCache.containsKey(query)) {
      PreparedStatement statement = mAdmin.getSession().prepare(query);
      mStatementCache.put(query, statement);
    }
    return mStatementCache.get(query);
  }
}
