package bumblebee.core.applier;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.util.MySQLConnectionManager;

public class MySQLConsumer implements Consumer {
	
	private MySQLPositionManager positionManager;
	
	public void setPositionManager(MySQLPositionManager positionManager) {
		this.positionManager = positionManager;
	}

	@Override public void consume(Event event) throws BusinessException {
		if (event.isInsert()) insert(event);
		if (event.isUpdate()) update(event);
		if (event.isDelete()) delete(event);
	}
	
	private void insert(Event event) throws BusinessException {
		executeSql(prepareInsertSQL(event));
	}
	
	private void update(Event event) throws BusinessException {
		executeSql(transformEventIntoUpdate(event));
	}

	private void delete(Event event) throws BusinessException {
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
	
	@Override public void commit() throws BusinessException {
		try {
			MySQLConnectionManager.getConsumerConnection().commit();
		} catch (SQLException ex) {
			throw new BusinessException(ex);
		}
	}
	
	@Override public void rollback() throws BusinessException {
		try {
			MySQLConnectionManager.getConsumerConnection().rollback();
		} catch (SQLException ex) {
			throw new BusinessException(ex);
		}
	}

	private void executeSql(String sql) throws BusinessException {
		try {
			Statement stmt = createStatement();
			stmt.executeUpdate(sql);
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}

	public Statement createStatement() throws SQLException, BusinessException {
		return MySQLConnectionManager.getConsumerConnection().createStatement();
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
