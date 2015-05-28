package bumblebee.core;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import bumblebee.core.applier.MySQLConsumer;

public class MySQLConsumerTest {

	@Test public void test() {
		MySQLConsumer consumer = new MySQLConsumer();
		Event event = new Event();
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("id", 1);
		event.setNamespace("database");
		event.setCollection("table");
		event.setData(data);
		assertEquals("INSERT INTO database.table SET id = '1';", consumer.prepareInsertSQL(event));
	}

}
