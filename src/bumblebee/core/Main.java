package bumblebee.core;

import java.io.IOException;

import bumblebee.core.applier.MySQLConsumer;
import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.reader.MySQLBinlogAdapter;
import bumblebee.core.reader.MySQLBinlogConnector;
import bumblebee.core.reader.MySQLSchemaManager;

public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");

		MySQLPositionManager positionManager = new MySQLPositionManager("db", "log_position");

		Consumer consumer = new MySQLConsumer();
		consumer.setPositionManager(positionManager);

		MySQLBinlogAdapter producer = new MySQLBinlogAdapter();
		producer.setSchemaManager(new MySQLSchemaManager());
		producer.attach(consumer);

		MySQLBinlogConnector connector = new MySQLBinlogConnector();
		connector.setAdapter(producer);
		connector.start();
	}
}
