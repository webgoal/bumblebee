package bumblebee.core.util;

import java.sql.Connection;
import java.sql.DriverManager;

import bumblebee.core.exceptions.BusinessException;

import com.typesafe.config.Config;

public class MySQLConnectionManager {
	private static Connection consumerConnection;
	private static Connection producerConnection;
	
	private static Config consumerConf;
	private static Config producerConf;
	
	public static void configLoader(Config config) {
		consumerConf = config.getConfig("consumer");
		producerConf = config.getConfig("producer");
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
	
	public static Connection getConsumerConnection() throws BusinessException {
		try {
			if(consumerConnection == null || consumerConnection.isClosed()) {
				consumerConnection = DriverManager.getConnection("jdbc:mysql://" + getConsumerHost() + ":" + getConsumerPort(), getConsumerUser(), getConsumerPass());
				consumerConnection.setAutoCommit(false);
			}
			return consumerConnection;
		} catch (Exception e) {
			throw new BusinessException(e);
		}
	}
	
	public static Connection getProducerConnection() throws BusinessException {
		try {
			if(producerConnection == null || producerConnection.isClosed()) {
				producerConnection = DriverManager.getConnection("jdbc:mysql://" + getProducerHost() + ":" + getProducerPort(), getProducerUser(), getProducerPass());
				producerConnection.setAutoCommit(false);
			}
			return producerConnection;
		} catch (Exception e) {
			throw new BusinessException(e);
		}
	}
}
