/**
 * 
 * Copyright (c) 2017 Fractus IT d.o.o. <http://fractus.io>
 * 
 */
package io.fractus.cassandra.client.java.dao;

import java.util.List;

import io.fractus.cassandra.client.java.model.Person;

public interface IPersonDao {

  public void insert(Person person);
  
  public List<Person> findAll();
  
}
