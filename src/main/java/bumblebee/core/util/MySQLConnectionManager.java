package bumblebee.core.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import bumblebee.core.exceptions.BusinessException;

public class MySQLConnectionManager {
	private static final int LOGIN_TIMEOUT = 5;
	
	private static Connection consumerConnection;
	private static Connection producerConnection;
	
	private static Config producerConf;
	private static Config consumerConf;

	public static void loadConfig(String configPath) {
		Config config = ConfigFactory.load(configPath);
		loadConfig(config);
	}
	
	public static void loadConfig(Config config) {
		producerConf = config.getConfig("source");
		consumerConf = config.getConfig("destination");
	}
	
	public static String getProducerHost() {
		return producerConf.getString("host");
	}
	
	public static String getProducerUser() {
		return producerConf.getString("user");
	}
	
	public static String getProducerPass() {
		return producerConf.getString("pass");
	}
	
	public static int getProducerPort() {
		return producerConf.getInt("port");
	}
	
	public static String getProducerDatabase() {
		return producerConf.getString("database");
	}
	
	public static int getProducerTimeout() {
		return producerConf.getInt("timeout");
	}
	
	public static String getConsumerHost() {
		return consumerConf.getString("host");
	}
	
	public static String getConsumerUser() {
		return consumerConf.getString("user");
	}
	
	public static String getConsumerPass() {
		return consumerConf.getString("pass");
	}
	
	public static int getConsumerPort() {
		return consumerConf.getInt("port");
	}
	
	public static String getConsumerDatabase() {
		return consumerConf.getString("database");
	}
	
	public static int getConsumerTimeout() {
		return consumerConf.getInt("timeout");
	}
	
	public static Connection getProducerConnection() {
		try {
			if (producerConnection == null || producerConnection.isClosed())
				producerConnection = getConnection(getProducerDatabase(), getProducerHost(), getProducerPort(), getProducerUser(), getProducerPass(), getProducerTimeout());
			return producerConnection;
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}
	
	public static Connection getConsumerConnection() {
		try {
			if (consumerConnection == null || consumerConnection.isClosed())
				consumerConnection = getConnection(getConsumerDatabase(), getConsumerHost(), getConsumerPort(), getConsumerUser(), getConsumerPass(), getConsumerTimeout());
			return consumerConnection;
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}

	private static Connection getConnection(String database, String host, int port, String user, String pass, int timeout) throws SQLException {
		DriverManager.setLoginTimeout(LOGIN_TIMEOUT);
		Connection consumerConnection = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "?connectTimeout=" + timeout + "&socketTimeout=" + timeout, user, pass);
		consumerConnection.setCatalog(database);
		consumerConnection.setAutoCommit(false);
		return consumerConnection;
	}
}
