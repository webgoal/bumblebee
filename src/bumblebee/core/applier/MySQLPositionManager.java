package bumblebee.core.applier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

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

	private String fullTable;
	private Connection connection;
	private Logger logger;

	public MySQLPositionManager(String db, String table) {
		logger = Logger.getLogger(getClass().getName());
		this.fullTable = db.isEmpty() ? table : db + "." + table;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public LogPosition getCurrentLogPosition() {
		try {
			Statement statement = connection.createStatement();
			ResultSet query = statement.executeQuery("SELECT binlog_filename, binlog_position FROM " + fullTable);
			query.first();
			return new LogPosition(query.getString("binlog_filename"), query.getLong("binlog_position"));
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}

	public void update(String logName, Long logPosition) {
		try {
			String sql = prepareUpdateSQL(logName, logPosition);
			logger.info("SQL: " + sql);
			Statement statement = connection.createStatement();
			statement.executeUpdate(sql);
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}

	private String prepareUpdateSQL(String logName, Long logPosition) {
		return "UPDATE " + fullTable + " SET binlog_filename = '" + logName + "', binlog_position = '" + logPosition + "'";
	}

}
