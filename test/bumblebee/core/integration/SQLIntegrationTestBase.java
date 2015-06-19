package bumblebee.core.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import bumblebee.core.exceptions.BusinessException;

public class SQLIntegrationTestBase {
	
	@Rule public ExpectedException assertThrown = ExpectedException.none();
	protected Connection connection = null;
	
	@After public void after() throws SQLException {
		if (connection != null && !connection.isClosed())
			connection.close();
	}
	
	protected Connection getFakeConnection() {
		try {
			connection = DriverManager.getConnection("jdbc:h2:mem:db1;MODE=MySQL");
			Statement statement = connection.createStatement();
			statement.executeUpdate("CREATE TABLE log_position (id INT, binlog_filename varchar(30), binlog_position LONG)");
			statement.executeUpdate("CREATE TABLE test (id INT AUTO_INCREMENT, name varchar(30))");
			statement.executeUpdate("INSERT INTO log_position VALUES (1, 'mysql-bin.000001', 4)");
			return connection;
		} catch (Exception e) {
			throw new BusinessException(e);
		}
	}

	protected void runSQL(String sql) {
		try {
			connection.createStatement().executeUpdate(sql);
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}

}
