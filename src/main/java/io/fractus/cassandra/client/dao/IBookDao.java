package io.fractus.cassandra.client.dao;

import java.util.List;

import io.fractus.cassandra.client.model.Book;

public interface IBookDao {

  public void insert(Book book);
  
  public List<Book> findAll();
  
}
