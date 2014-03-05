package org.kiji.schema.impl.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.util.HashMap;
import java.util.Map;

public class CassandraStatementCache {
  private final Session mSession;

  private final Map<String, PreparedStatement> mStatementCache;

  CassandraStatementCache(Session session) {
    mSession = session;
    mStatementCache = new HashMap<String, PreparedStatement>();
  }

  synchronized PreparedStatement getPreparedStatement(String query) {
    if (!mStatementCache.containsKey(query)) {
      PreparedStatement statement = mSession.prepare(query);
      mStatementCache.put(query, statement);
    }
    return mStatementCache.get(query);
  }
}
