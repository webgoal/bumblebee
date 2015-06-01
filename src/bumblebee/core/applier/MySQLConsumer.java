package bumblebee.core.applier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import bumblebee.core.Event;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;

public class MySQLConsumer implements Consumer {
	private static final String host     = "192.168.59.103";
	private static final String user     = "root";
	private static final String pass     = "mypass";
	private static final Integer port    = 3307;
	private final String url;
	private MySQLPositionManager positionManager;
	
	public MySQLConsumer() {
		this.url = "jdbc:mysql://" + host + ":" + port;
	}
	
	public void setPositionManager(MySQLPositionManager positionManager) {
		this.positionManager = positionManager;
		try {
			positionManager.setConnection(DriverManager.getConnection(url, user, pass));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override public void insert(Event event) throws BusinessException {
		executeSql(prepareInsertSQL(event));
	}
	
	@Override public void update(Event event) throws BusinessException {
		executeSql(transformEventIntoUpdate(event));
	}

	@Override public void delete(Event event) throws BusinessException {
		executeSql(transformEventIntoDelete(event));
	}
	
	@Override public void setPosition(String logName, long logPosition) throws BusinessException {
		positionManager.update(logName, logPosition);
	}
	
	@Override public void setPosition(long logPosition) throws BusinessException {
		positionManager.update(getCurrentLogPosition().getFilename(), logPosition);
	}
	
	@Override public LogPosition getCurrentLogPosition() throws BusinessException {
		return positionManager.getCurrentLogPosition();
	}

	private void executeSql(String sql) throws BusinessException {
		System.out.println("SQL: " + sql);
		try {
			Connection connection  = DriverManager.getConnection(url, user, pass);
			Statement stmt = connection.createStatement();
			stmt.executeUpdate(sql);
		} catch (Exception e) {
			throw new BusinessException(e);
		}
	}

	public String prepareInsertSQL(Event event) {
		return "INSERT INTO " + databaseAndTable(event) + " SET " + fieldsAndValues(event);
	}

	public String transformEventIntoUpdate(Event event) {
		return "UPDATE " + databaseAndTable(event) + " SET " + fieldsAndValues(event) + " WHERE " + conditions(event);
	}

	public String transformEventIntoDelete(Event event) {
		return "DELETE FROM " + databaseAndTable(event) + " WHERE " + conditions(event);
	}

	private String databaseAndTable(Event event) {
		return event.getNamespace() + "." + event.getCollection();
	}

	private String fieldsAndValues(Event event) {
		return serializeMap(event.getData(), ", ");
	}

	private String conditions(Event event) {
		return serializeMap(event.getConditions(), " AND ");
	}
	
	private String serializeMap(Map<String, Object> data, String glue) {
		StringBuffer sb = new StringBuffer();
		data.forEach((k,v) -> sb.append(k + " = '" + v + "'" + glue));
		sb.replace(sb.length() - glue.length(), sb.length(), "");
		return sb.toString();
	}
}
