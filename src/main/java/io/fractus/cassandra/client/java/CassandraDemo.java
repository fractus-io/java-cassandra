/**
 * 
 * Copyright (c) 2017 Fractus IT d.o.o. <http://fractus.io>
 * 
 */
package io.fractus.cassandra.client.java;

import java.util.List;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

import io.fractus.cassandra.client.java.connector.CassandraConnector;
import io.fractus.cassandra.client.java.connector.CassandraTemplate;
import io.fractus.cassandra.client.java.dao.BookDao;
import io.fractus.cassandra.client.java.dao.IBookDao;
import io.fractus.cassandra.client.java.model.Book;

public class CassandraDemo {

  public static void main(String[] args) {
    
    String keyspaceName = "library";
       
    // connect to cassandra cluster
    CassandraConnector cassandraConnector = new CassandraConnector();
    Session session = cassandraConnector.connect("127.0.0.1", null);
    
    System.out.println("connected to cassandra cluster ");
    
    CassandraTemplate cassandraTemplate = new CassandraTemplate(session);
    
    // create library keyspace -> in fact keyspace is container
    cassandraTemplate.createKeyspace(keyspaceName);
    cassandraTemplate.useKeyspace(keyspaceName);
    
    // create table book
    cassandraTemplate.createTableBook();

    // prepare book object
    Book book = new Book();
    book.setId(UUIDs.timeBased());
    book.setAuthor("author");
    book.setTitle("title");
    book.setSubject("subject");
    book.setPublisher("publisher");
    
    // initialize IBookDao
    IBookDao bookDao = new BookDao(session);
    
    // insert into book table
    bookDao.insert(book);
    
    List<Book> books = bookDao.findAll();
    for (Book aBook : books) {
      System.out.println(aBook.getId());
      System.out.println(aBook.getAuthor());
      System.out.println(aBook.getTitle());
      System.out.println(aBook.getSubject());
      System.out.println(aBook.getPublisher());
    }
    
    // delete keyspace
    cassandraTemplate.deleteKeyspace(keyspaceName);
    session.close();
    cassandraConnector.close();
  }

}
