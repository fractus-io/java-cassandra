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

import io.fractus.cassandra.client.java.model.Person;

public class PersonDao  implements IPersonDao {

  private Session session;
  
  public PersonDao(final Session session) {
    this.session = session;
  }
  
  public void insert(Person person) {
    
    StringBuilder sb = new StringBuilder("INSERT INTO PERSON ")
                            .append("(id, firstName, familyName, age) ")
                            .append("VALUES (")
                            .append(person.getId()).append(", '")
                            .append(person.getFirstName())
                            .append("', '")
                            .append(person.getFamilyName())
                            .append("', ")
                            .append(person.getAge())
                            .append(");");

     final String query = sb.toString();
     
     session.execute(query);
  }

  
  public List<Person> findAll() {
    
    StringBuilder sb = new StringBuilder("SELECT * FROM PERSON");

    final String query = sb.toString();
    ResultSet resultSet = session.execute(query);

    List<Person> persons = new ArrayList<Person>();

    for (Row row : resultSet) {
        Person person = new Person();
        person.setId(row.getUUID("id"));
        person.setFirstName(row.getString("firstName"));
        person.setFamilyName(row.getString("familyName"));
        person.setAge(row.getInt("age"));
        
        persons.add(person);
    }
    return persons;  
  }

}
