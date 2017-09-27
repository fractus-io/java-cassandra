package io.fractus.cassandra.client.connector;

import com.datastax.driver.core.Session;

public class CassandraTemplate {

  private Session session;
  
  public CassandraTemplate(final Session session) {
    this.session = session;
  }
  
  public void createKeyspace(final String keyspaceName, final String replicationStrategy, final int numberOfReplicas) {
    
    StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                          .append(keyspaceName)
                          .append(" WITH replication = {")
                          .append("'class':'")
                          .append(replicationStrategy)
                          .append("','replication_factor':")
                          .append(numberOfReplicas).append("};");

    final String query = sb.toString();

    this.session.execute(query);
  }

  /*
   * default values -> typical for development cluster:
   * replicationStragetgy = SimpleStrategy
   * numberOfReplicas = 1
   * 
   */
  public void createKeyspace(final String keyspaceName) {
    this.createKeyspace(keyspaceName, "SimpleStrategy", 1);
  }

  public void useKeyspace(final String keyspaceName) {
    this.session.execute("USE " + keyspaceName);
  }
  
  public void deleteKeyspace(final String keyspaceName) {
    this.session.execute("DROP KEYSPACE " + keyspaceName);    
  }
  
  public void createTableBook() {
    
    StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS BOOK ")
                          .append("(")
                          .append("id uuid PRIMARY KEY, ")
                          .append("title text,")
                          .append("author text,")
                          .append("subject text,")
                          .append("publisher text);");

    final String query = sb.toString();
    session.execute(query);
  }
}
