package bumblebee.core.applier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import bumblebee.core.exceptions.BusinessException;

public class MySQLPositionManager {
	
	public static class LogPosition {
		private String filename;
		private Long position;
		public LogPosition(String filename, Long position) {
			this.filename = filename;
			this.position = position;
		}
		public String getFilename() { return filename; }
		public Long getPosition() { return position; }
	}

	private String db;
	private String table;
	private Connection connection;

	public MySQLPositionManager(String db, String table) {
		this.db = db;
		this.table = table;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	
	public LogPosition getCurrentLogPosition() throws BusinessException {
		try {
			Statement statement = connection.createStatement();
			ResultSet query = statement.executeQuery("SELECT binlog_filename, binlog_position FROM " + db + "." + table);
			query.first();
			return new LogPosition(query.getString("binlog_filename"), query.getLong("binlog_position"));
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}

	public void update(String logName, Long logPosition) throws BusinessException {
		try {
			String sql = prepareUpdateSQL(logName, logPosition);
			System.out.println("SQL: " + sql);
			Statement statement = connection.createStatement();
			statement.executeUpdate(sql);
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}

	private String prepareUpdateSQL(String logName, Long logPosition) {
		return "UPDATE " + db + "." + table + " SET binlog_filename = '" + logName + "', binlog_position = '" + logPosition + "'";
	}

}
