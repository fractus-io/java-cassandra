package io.fractus.cassandra.client;

import java.util.List;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;

import io.fractus.cassandra.client.connector.CassandraConnector;
import io.fractus.cassandra.client.connector.CassandraTemplate;
import io.fractus.cassandra.client.dao.BookDao;
import io.fractus.cassandra.client.dao.IBookDao;
import io.fractus.cassandra.client.model.Book;

public class CassandraDemo {

  public static void main(String[] args) {
    
    String keyspaceName = "library";
    
    System.out.println("here we go");
    
    CassandraConnector cassandraConnector = new CassandraConnector();
    Session session = cassandraConnector.connect("127.0.0.1", null);
    
    System.out.println("connected to cassandra cluster ");
    
    // connect to cassandra cluster
    CassandraTemplate cassandraTemplate = new CassandraTemplate(session);
    
    // create library keyspace -> in fact keyspace is container
    cassandraTemplate.createKeyspace(keyspaceName);
    cassandraTemplate.useKeyspace(keyspaceName);
    
    // create table book
    cassandraTemplate.createTableBook();

    // prepare book
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
    System.out.println(books);
    
    // delete keyspace
    cassandraTemplate.deleteKeyspace(keyspaceName);
    session.close();
    cassandraConnector.close();
  }

}
