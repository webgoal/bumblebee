package bumblebee.samples.simple_application;

import java.io.IOException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import bumblebee.core.applier.MySQLConsumer;
import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Transformer;
import bumblebee.core.reader.MySQLBinlogAdapter;
import bumblebee.core.reader.MySQLBinlogConnector;
import bumblebee.core.reader.MySQLSchemaManager;
import bumblebee.core.util.MySQLConnectionManager;
import bumblebee.samples.transformations.MySQLDelegateTransformer;

public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException, BusinessException {
		Class.forName("com.mysql.jdbc.Driver");
		
		Config config = ConfigFactory.load("bumblebee/samples/simple_application/application.conf");
		MySQLConnectionManager.loadConfig(config);
		
		MySQLPositionManager positionManager = new MySQLPositionManager("db", "log_position");
		positionManager.setConnection(MySQLConnectionManager.getConsumerConnection());

		Consumer consumer = new MySQLConsumer();
		consumer.setPositionManager(positionManager);

		Transformer tr = new MySQLDelegateTransformer();

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
