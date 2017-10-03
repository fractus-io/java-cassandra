/**
 * 
 * Copyright (c) 2017 Fractus IT d.o.o. <http://fractus.io>
 * 
 */
package io.fractus.cassandra.client.java;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

import io.fractus.cassandra.client.java.connector.CassandraConnector;
import io.fractus.cassandra.client.java.connector.CassandraTemplate;
import io.fractus.cassandra.client.java.dao.BookDao;
import io.fractus.cassandra.client.java.dao.IBookDao;
import io.fractus.cassandra.client.java.model.Book;

public class CassandraClientJavaTest {
  
  private IBookDao bookDao;
  
  @BeforeClass
  public static void init() throws ConfigurationException, TTransportException, IOException, InterruptedException {
      // Start an embedded Cassandra Server
      EmbeddedCassandraServerHelper.startEmbeddedCassandra(20000L);
  }
  
  @Before
  public void before() {
    
    // connect to cassandra cluster
    CassandraConnector cassandraConnector = new CassandraConnector();
    Session session = cassandraConnector.connect("127.0.0.1", null);
        
    CassandraTemplate cassandraTemplate = new CassandraTemplate(session);
    
    String keyspaceName = "library";

    // create library keyspace -> in fact keyspace is container
    cassandraTemplate.createKeyspace(keyspaceName);
    cassandraTemplate.useKeyspace(keyspaceName);
    
    // create table book
    cassandraTemplate.createTableBook();
    
    // initialize IBookDao
    this.bookDao = new BookDao(session);
  }
  
  
  @Test
  public void testBook() {

    // prepare book object
    Book book = new Book();
    book.setId(UUIDs.timeBased());
    book.setAuthor("author");
    book.setTitle("title");
    book.setSubject("subject");
    book.setPublisher("publisher");
        
    // insert into book table
    bookDao.insert(book);
    
    List<Book> books = bookDao.findAll();
    for (Book aBook : books) {
      assertEquals("author", aBook.getAuthor());
      assertEquals("title", aBook.getTitle());
      assertEquals("subject", aBook.getSubject());
      assertEquals("publisher", aBook.getPublisher());
    }
  }
  
  @AfterClass
  public static void cleanup() {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }  
}
