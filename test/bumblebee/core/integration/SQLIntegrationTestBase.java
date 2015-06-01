package bumblebee.core.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;

public class SQLIntegrationTestBase {

	private Connection connection = null;
	
	@After public void after() throws SQLException {
		if (connection != null && !connection.isClosed())
			connection.close();
	}
	
	protected Connection getFakeConnection() {
		try {
			connection = DriverManager.getConnection("jdbc:h2:mem:;MODE=MySQL");
			Statement statement = connection.createStatement();
			statement.executeUpdate("CREATE SCHEMA db");
			statement.executeUpdate("CREATE TABLE db.log_position (id INT, binlog_filename varchar(30), binlog_position LONG)");
			statement.executeUpdate("INSERT INTO db.log_position VALUES (1, 'mysql-bin.000001', 4)");
			return connection;
		} catch (Exception e) {
			System.err.println("SQLIntegrationTestBase: Error creating FakeConnection.");
			e.printStackTrace();
		}
		return null;
	}

}
