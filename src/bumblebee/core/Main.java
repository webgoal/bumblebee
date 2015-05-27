package bumblebee.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import bumblebee.core.aux.DummyConsumer;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.MySQLSchemaManager;
import bumblebee.core.reader.MySQLBinlogAdapter;
import bumblebee.core.reader.MySQLBinlogConnector;

public class Main {
	public static void main(String[] args) throws IOException {
		Consumer consumer = new DummyConsumer();
		MySQLBinlogAdapter producer = new MySQLBinlogAdapter();
		producer.setSchemaManager(new TestSchemaManager());
		producer.attach(consumer);
		
		MySQLBinlogConnector connector = new MySQLBinlogConnector();
		connector.setAdapter(producer);
		connector.start();
	}
}

class TestSchemaManager implements MySQLSchemaManager {
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