package bumblebee.core;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import bumblebee.core.applier.MySQLConsumer;
import bumblebee.core.events.Event;
import bumblebee.core.events.InsertEvent;
import bumblebee.core.exceptions.BusinessException;

public class MySQLConsumerTest {
	
	private MySQLConsumer consumer = new MySQLConsumer(null);
	private Map<String, Object> data = new HashMap<String, Object>();
	private Map<String, Object> conditions = new HashMap<String, Object>();
	
	@Before public void setup() {
		data.put("id", 1);
		conditions.put("id", 2);
	}

	@Test public void eventToSQLInsertTransformationTest() throws BusinessException {
		InsertEvent insertEvent = new InsertEvent();
		insertEvent.setNamespace("database");
		insertEvent.setCollection("table");
		insertEvent.setData(data);
		
//		consumer.consume(insertEvent);
//		assertEquals("INSERT INTO database.table SET id = '1'", consumer.prepareInsertSQL(event));
	}
	
//	@Test public void eventToSQLUpdateTransformationTest() {
//		assertEquals("UPDATE database.table SET id = '1' WHERE id = '2'", consumer.transformEventIntoUpdate(event));
//	}
//
//	@Test public void eventWithMultipleDataToSQLUpdateTransformationTest() {
//		data.put("name", "actual name");
//		conditions.put("name", "previous name");
//		event.setData(data);
//		event.setConditions(conditions);
//		assertEquals("UPDATE database.table SET name = 'actual name', id = '1' WHERE name = 'previous name' AND id = '2'", consumer.transformEventIntoUpdate(event));
//	}
//	
//	@Test public void eventToSQLDeleteTransformationTest() {
//		assertEquals("DELETE FROM database.table WHERE id = '2'", consumer.transformEventIntoDelete(event));
//	}

}
