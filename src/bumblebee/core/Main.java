package bumblebee.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import bumblebee.core.aux.DummyConsumer;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.reader.MySQLBinlogReader;
import bumblebee.core.reader.SchemaManager;

public class Main {
	public static void main(String[] args) throws IOException {
		Consumer consumer = new DummyConsumer();
		MySQLBinlogReader producer = new MySQLBinlogReader();
		producer.setSchemaManager(new TestSchemaManager());
		producer.attach(consumer);
		producer.start();
	}
}

class TestSchemaManager implements SchemaManager {
	private Map<String, List<String>> tableSchemas = new HashMap<String, List<String>>();
	LinkedList<String> cols = new LinkedList<String>();		
	public TestSchemaManager() {
		cols.add("id");
		cols.add("name");
		tableSchemas.put("test", cols);
	}
	@Override public String getColumnName(String tableName, int index) {
		return tableSchemas.get(tableName).get(index);
	}
}