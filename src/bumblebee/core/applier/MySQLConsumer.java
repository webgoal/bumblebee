package bumblebee.core.applier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.util.MySQLConnectionManager;

public class MySQLConsumer implements Consumer {
	
	private Connection connection;
	private MySQLPositionManager positionManager;
	
	public MySQLConsumer(Connection connection, MySQLPositionManager positionManager) {
		this.connection = connection;
		this.positionManager = positionManager;
	}
	
	@Override public void consume(Event event) throws BusinessException {
		if (event.isInsert()) insert(event);
		if (event.isUpdate()) update(event);
		if (event.isDelete()) delete(event);
	}
	
	private void insert(Event event) throws BusinessException {
		executeSql(prepareInsertSQL(event), event.getData().values(), Collections.emptyList());
	}
	
	private void update(Event event) throws BusinessException {
		executeSql(prepareUpdateSQL(event), event.getData().values(), event.getConditions().values());
	}

	private void delete(Event event) throws BusinessException {
		executeSql(prepareDeleteSQL(event), Collections.emptyList(), event.getConditions().values());
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

	private void executeSql(String sql, Collection<Object> data, Collection<Object> conditions) throws BusinessException {
		try {
			System.out.println("SQL: " + sql);
			PreparedStatement stmt = connection.prepareStatement(sql);
			int counter = 1;
			for (Object v : data)       stmt.setObject(counter++, fixType(v));
			for (Object v : conditions) stmt.setObject(counter++, fixType(v));
			stmt.executeUpdate();
		} catch (SQLException e) {
			throw new BusinessException(e);
		}
	}

	private Object fixType(Object object) {
		if (object instanceof byte[]) return new String((byte[]) object);
		return object;
	}

	private String prepareInsertSQL(Event event) throws BusinessException {
		return "INSERT INTO " + databaseAndTable(event) + " SET " + fields(event);
	}

	private String prepareUpdateSQL(Event event) throws BusinessException {
		return "UPDATE " + databaseAndTable(event) + " SET " + fields(event) + " WHERE " + conditions(event);
	}

	private String prepareDeleteSQL(Event event) {
		return "DELETE FROM " + databaseAndTable(event) + " WHERE " + conditions(event);
	}

	private String databaseAndTable(Event event) {
		return event.getNamespace() + "." + event.getCollection();
	}

	private String fields(Event event) throws BusinessException {
		return serializeMap(event.getData(), ", ");
	}

	private String conditions(Event event) {
		return serializeMap(event.getConditions(), " AND ");
	}
	
	private String serializeMap(Map<String, Object> data, String glue) {
		StringBuffer sb = new StringBuffer();
		data.forEach((k, v) -> sb.append(k + " = ?" + glue));
		sb.replace(sb.length() - glue.length(), sb.length(), "");
		return sb.toString();
	}
}
