package bumblebee.core;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import bumblebee.core.applier.RESTConsumer;
import bumblebee.core.applier.rest.JSONEventTranslator;
import bumblebee.core.events.InsertEvent;

public class RESTConsumerTest {
	private Map<String, Object> data = new HashMap<String, Object>();
	private Map<String, Object> conditions = new HashMap<String, Object>();
	
	@Before public void setup() {
		data.put("name", "Someone");
		data.put("id", 1);
		conditions.put("id", 2);
	}

	@Test public void eventToPUTTransformationTest() {
		RESTConsumer consumer = new RESTConsumer(new JSONEventTranslator());
		
		InsertEvent insertEvent = new InsertEvent();
		insertEvent.setNamespace("database");
		insertEvent.setCollection("table");
		insertEvent.setData(data);
		
		consumer.consume(insertEvent);
		
//		verify(connection).prepareStatement("INSERT INTO database.table SET name = ?, id = ?");
//		verify(statement).setObject(1, "Someone");
//		verify(statement).setObject(2, 1);
//		verify(statement).executeUpdate();
	}
}
