package bumblebee.core.applier;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import util.ConnectionManager;
import bumblebee.core.Event;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;

public class MySQLConsumer implements Consumer {
	
	private MySQLPositionManager positionManager;
	private ConnectionManager connectionManager;
	
	public void setPositionManager(MySQLPositionManager positionManager) throws BusinessException {
		this.positionManager = positionManager;
		positionManager.setConnection(connectionManager.getConsumerConnection());
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
			Statement stmt = connectionManager.getConsumerConnection().createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
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

	@Override
	public void setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}
}
