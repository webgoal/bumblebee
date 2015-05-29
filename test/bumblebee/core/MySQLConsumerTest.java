package bumblebee.core;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import bumblebee.core.applier.MySQLConsumer;

public class MySQLConsumerTest {
	
	private MySQLConsumer consumer = new MySQLConsumer();
	private Event event = new Event();
	private Map<String, Object> data = new HashMap<String, Object>();
	private Map<String, Object> conditions = new HashMap<String, Object>();
	
	@Before public void setup() {
		event.setNamespace("database");
		event.setCollection("table");
		
		data.put("id", 1);
		event.setData(data);
		
		conditions.put("id", 2);
		event.setConditions(conditions);
	}

	@Test public void eventToSQLInsertTransformationTest() {
		assertEquals("INSERT INTO database.table SET id = '1'", consumer.prepareInsertSQL(event));
	}
	
	@Test public void eventToSQLUpdateTransformationTest() {
		assertEquals("UPDATE database.table SET id = '1' WHERE id = '2'", consumer.transformEventIntoUpdate(event));
	}

	@Test public void eventWithMultipleDataToSQLUpdateTransformationTest() {
		data.put("name", "actual name");
		conditions.put("name", "previous name");
		event.setData(data);
		event.setConditions(conditions);
		assertEquals("UPDATE database.table SET name = 'actual name', id = '1' WHERE name = 'previous name' AND id = '2'", consumer.transformEventIntoUpdate(event));
	}
	
	@Test public void eventToSQLDeleteTransformationTest() {
		assertEquals("DELETE FROM database.table WHERE id = '2'", consumer.transformEventIntoDelete(event));
	}

}
