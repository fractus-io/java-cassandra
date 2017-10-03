/**
 * 
 * Copyright (c) 2017 Fractus IT d.o.o. <http://fractus.io>
 * 
 */
package io.fractus.cassandra.client.java.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

public class CassandraConnector {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraConnector.class);
  
  private Cluster cluster;
  
  public Session connect(final String node, final Integer port) {
    
    try {
      Builder builder = Cluster.builder().addContactPoint(node);
      
      if(port != null) {
        builder.withPort(port);
      }
      
      this.cluster = builder.build();
      LOG.info("Cluster name: " + this.cluster.getMetadata().getClusterName());
          
      return this.cluster.connect();
    }catch (Throwable e){
      
      LOG.error(e.getMessage());
     
      throw new IllegalStateException("Cannot connect to Cassandra Cluster");
    }
  }
  
  public void close() {
    this.cluster.close();
  }
}
