package util;

import java.sql.Connection;
import java.sql.DriverManager;

import bumblebee.core.exceptions.BusinessException;

public class MySQLConnectionManager {
	private static final String  producerHost = "192.168.59.103";
	private static final String  producerUser = "root";
	private static final String  producerPass = "mypass";
	private static final Integer producerPort = 3306;
	private static final String producerUrl = "jdbc:mysql://" + producerHost + ":" + producerPort;
	
	private static final String  consumerHost = "192.168.59.103";
	private static final String  consumerUser = "root";
	private static final String  consumerPass = "mypass";
	private static final Integer consumerPort = 3307;
	private static final String  consumerUrl  = "jdbc:mysql://" + consumerHost + ":" + consumerPort;
	private static Connection consumerConnection;
	private static Connection producerConnection;
	
	public static String getProducerHost() {
		return producerHost;
	}
	
	public static String getProducerUser() {
		return producerUser;
	}
	
	public static String getProducerPass() {
		return producerPass;
	}
	
	public static Integer getProducerPort() {
		return producerPort;
	}
	
	public Connection getConsumerConnection() throws BusinessException {
		try {
			if(consumerConnection == null || consumerConnection.isClosed()) {
				consumerConnection = DriverManager.getConnection(consumerUrl, consumerUser, consumerPass);
				consumerConnection.setAutoCommit(false);
			}
			return consumerConnection;
		} catch (Exception e) {
			throw new BusinessException(e);
		}
	}
	
	public Connection getProducerConnection() throws BusinessException {
		System.out.println("oi");
		try {
			if(producerConnection == null || producerConnection.isClosed()) {
				producerConnection = DriverManager.getConnection(producerUrl, producerUser, producerPass);
				producerConnection.setAutoCommit(false);
			}
			return producerConnection;
		} catch (Exception e) {
			throw new BusinessException(e);
		}
	}
}
