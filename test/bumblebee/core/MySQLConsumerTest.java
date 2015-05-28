package bumblebee.core;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import bumblebee.core.applier.MySQLConsumer;

public class MySQLConsumerTest {
	
	private MySQLConsumer consumer = new MySQLConsumer();
	private Event event = new Event();
	private Map<String, Object> data = new HashMap<String, Object>();

	@Test public void eventToSQLInsertTransformationTest() {
		data.put("id", 1);
		event.setNamespace("database");
		event.setCollection("table");
		event.setData(data);

		assertEquals("INSERT INTO database.table SET id = '1'", consumer.prepareInsertSQL(event));
	}
	
	@Test public void eventToSQLUpdateTransformationTest() {
		Map<String, Object> conditions = new HashMap<String, Object>();

		conditions.put("id", 2);
		data.put("id", 1);
		event.setData(data);
		event.setConditions(conditions);
		event.setNamespace("database");
		event.setCollection("table");

		assertEquals("UPDATE database.table SET id = '1' WHERE id = '2'", consumer.transformEventIntoUpdate(event));
	}
	
	@Test public void eventToSQLDeleteTransformationTest() {
		Map<String, Object> conditions = new HashMap<String, Object>();

		conditions.put("id", 2);
		event.setConditions(conditions);
		event.setNamespace("database");
		event.setCollection("table");

		assertEquals("DELETE FROM database.table WHERE id = '2'", consumer.transformEventIntoDelete(event));
	}

}
