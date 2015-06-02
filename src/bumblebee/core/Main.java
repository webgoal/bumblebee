package bumblebee.core;

import java.io.IOException;

import bumblebee.core.applier.MySQLConsumer;
import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Transformer;
import bumblebee.core.reader.MySQLBinlogAdapter;
import bumblebee.core.reader.MySQLBinlogConnector;
import bumblebee.core.reader.MySQLSchemaManager;
import bumblebee.transformer.DelegateTransformer;

public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException, BusinessException {
		Class.forName("com.mysql.jdbc.Driver");
		
		MySQLPositionManager positionManager = new MySQLPositionManager("db", "log_position");

		Consumer consumer = new MySQLConsumer();
		consumer.setPositionManager(positionManager);

		Transformer tr = new DelegateTransformer();

		tr.attach(consumer);

		MySQLBinlogAdapter producer = new MySQLBinlogAdapter();
		MySQLSchemaManager schemaManager = new MySQLSchemaManager();
		producer.setSchemaManager(schemaManager);
		producer.attach(tr);

		MySQLBinlogConnector connector = new MySQLBinlogConnector(positionManager.getCurrentLogPosition());		
		connector.setAdapter(producer);
		connector.start();
	}
}
