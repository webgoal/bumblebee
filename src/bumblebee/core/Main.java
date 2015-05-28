package bumblebee.core;

import java.io.IOException;

import bumblebee.core.applier.MySQLConsumer;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.reader.MySQLBinlogAdapter;
import bumblebee.core.reader.MySQLBinlogConnector;
import bumblebee.core.reader.MySQLSchemaManager;

public class Main {
	public static void main(String[] args) throws IOException {
		Consumer consumer = new MySQLConsumer();
		MySQLBinlogAdapter producer = new MySQLBinlogAdapter();
		producer.setSchemaManager(new MySQLSchemaManager());
		producer.attach(consumer);
		
		MySQLBinlogConnector connector = new MySQLBinlogConnector();
		connector.setAdapter(producer);
		connector.start();
	}
}
