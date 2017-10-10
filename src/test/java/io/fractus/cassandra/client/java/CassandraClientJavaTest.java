/**
 * 
 * Copyright (c) 2017 Fractus IT d.o.o. <http://fractus.io>
 * 
 */
package io.fractus.cassandra.client.java;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

import io.fractus.cassandra.client.java.connector.CassandraConnector;
import io.fractus.cassandra.client.java.dao.BookDao;
import io.fractus.cassandra.client.java.dao.IBookDao;
import io.fractus.cassandra.client.java.model.Book;

public class CassandraClientJavaTest {
  
  private Session session;
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
    this.session = cassandraConnector.connect("localhost", 9142);
   
    this.session.execute("CREATE KEYSPACE IF NOT EXISTS library WITH replication = {'class':'SimpleStrategy','replication_factor':1};");
    this.session.execute("USE library;");
    this.session.execute("CREATE TABLE IF NOT EXISTS BOOK (id uuid PRIMARY KEY, title text, author text, subject text, publisher text);");
    
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
  
  @After
  public void after() {
    this.session.execute("DROP KEYSPACE library;");
  }
  
  @AfterClass
  public static void cleanup() {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }  
}
