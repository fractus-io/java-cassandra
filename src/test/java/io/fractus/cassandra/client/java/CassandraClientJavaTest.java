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
import io.fractus.cassandra.client.java.dao.PersonDao;
import io.fractus.cassandra.client.java.dao.IPersonDao;
import io.fractus.cassandra.client.java.model.Person;

public class CassandraClientJavaTest {
  
  private Session session;
  private IPersonDao personDao;
  
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
   
    this.session.execute("CREATE KEYSPACE IF NOT EXISTS archive WITH replication = {'class':'SimpleStrategy','replication_factor':1};");
    this.session.execute("USE archive;");
    this.session.execute("CREATE TABLE IF NOT EXISTS PERSON (id uuid PRIMARY KEY, firstName text, familyName text, age int);");
    
    // initialize IBookDao
    this.personDao = new PersonDao(session);
  }
  
  
  @Test
  public void testCRUDPerson() {

    // prepare book object
    Person person = new Person();
    person.setId(UUIDs.timeBased());
    person.setFirstName("firstName");
    person.setFamilyName("familyName");
    person.setAge(99);
        
    // insert into person table
    personDao.insert(person);
    
    List<Person> persons = personDao.findAll();
    for (Person aPerson : persons) {
      assertEquals("firstName", aPerson.getFirstName());
      assertEquals("familyName", aPerson.getFamilyName());
      assertEquals(99, aPerson.getAge());
    }
  }
  
  @After
  public void after() {
    this.session.execute("DROP KEYSPACE archive;");
  }
  
  @AfterClass
  public static void cleanup() {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }  
}
