package bumblebee.core.applier.rest;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import bumblebee.core.events.InsertEvent;

public class JSONEventTranslatorTest {
	private Map<String, Object> data = new LinkedHashMap<String, Object>();
	private Map<String, Object> conditions = new LinkedHashMap<String, Object>();

	@Before
	public void setup() {
		data.put("id", 1);
		data.put("name", "Someone");
		conditions.put("id", 2);
	}

	@Test public void eventToTJSONTest() {
		JSONEventTranslator translator = new JSONEventTranslator();

		InsertEvent insertEvent = new InsertEvent();
		insertEvent.setNamespace("database");
		insertEvent.setCollection("table");
		insertEvent.setData(data);

		String expectedJson = "{\"id\":1,\"name\":\"Someone\"}";

		assertEquals(expectedJson, translator.toJson(insertEvent));
	}
}
