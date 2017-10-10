/**
 * 
 * Copyright (c) 2017 Fractus IT d.o.o. <http://fractus.io>
 * 
 */
package io.fractus.cassandra.client.java.dao;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import io.fractus.cassandra.client.java.model.Book;

public class BookDao  implements IBookDao {

  private Session session;
  
  public BookDao(final Session session) {
    this.session = session;
  }
  
  public void insert(Book book) {
    
    StringBuilder sb = new StringBuilder("INSERT INTO BOOK ")
                            .append("(id, title, author, subject, publisher) ")
                            .append("VALUES (")
                            .append(book.getId()).append(", '")
                            .append(book.getTitle())
                            .append("', '")
                            .append(book.getAuthor())
                            .append("', '")
                            .append(book.getSubject())
                            .append("', '")
                            .append(book.getPublisher())
                            .append("');");

     final String query = sb.toString();
     
     session.execute(query);
  }

  
  public List<Book> findAll() {
    
    StringBuilder sb = new StringBuilder("SELECT * FROM BOOK");

    final String query = sb.toString();
    ResultSet resultSet = session.execute(query);

    List<Book> books = new ArrayList<Book>();

    for (Row r : resultSet) {
        Book book = new Book();
        book.setId(r.getUUID("id"));
        book.setTitle(r.getString("title"));
        book.setAuthor(r.getString("author"));
        book.setSubject(r.getString("subject"));
        book.setPublisher(r.getString("publisher"));
        books.add(book);
    }
    return books;  
  }

}
