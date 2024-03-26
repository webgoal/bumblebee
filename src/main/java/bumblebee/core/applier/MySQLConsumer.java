package bumblebee.core.applier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;


public class MySQLConsumer extends AbstractConsumer {

	private Connection connection;
	private MySQLPositionManager positionManager;
	private Logger logger;

	public MySQLConsumer(Connection connection, MySQLPositionManager positionManager) {
		logger = Logger.getLogger(getClass().getName());
		this.connection = connection;
		this.positionManager = positionManager;
	}

	protected void insert(Event event) {
		executeSql(prepareInsertSQL(event), event.getData().values(), Collections.emptyList());
	}

	protected void update(Event event) {
		executeSql(prepareUpdateSQL(event), event.getData().values(), event.getConditions().values());
	}

	protected void delete(Event event) {
		executeSql(prepareDeleteSQL(event), Collections.emptyList(), event.getConditions().values());
	}

	@Override public void setPosition(String logName, long logPosition) {
		positionManager.update(logName, logPosition);
	}

	@Override public void setPosition(long logPosition) {
		positionManager.update(getCurrentLogPosition().getFilename(), logPosition);
	}

	@Override public LogPosition getCurrentLogPosition() {
		return positionManager.getCurrentLogPosition();
	}

	@Override public void commit() {
		try {
			connection.commit();
		} catch (SQLException ex) {
			throw new BusinessException(ex);
		}
	}

	@Override public void rollback() {
		try {
			connection.rollback();
		} catch (SQLException ex) {
			throw new BusinessException(ex);
		}
	}

	private void executeSql(String sql, Collection<Object> data, Collection<Object> conditions) {
		try {
			logger.info("SQL: " + sql);
			PreparedStatement stmt = connection.prepareStatement(sql);
			int counter = 1;
			for (Object v : data)       stmt.setObject(counter++, fixType(v));
			for (Object v : conditions) stmt.setObject(counter++, fixType(v));
			logger.info(stmt.toString());
			stmt.executeUpdate();
			stmt.close();
		} catch (SQLException e) {
			logger.severe("Falha ao executar SQL");
			throw new BusinessException(e);
		}
	}

	private Object fixType(Object object) {
		if (object instanceof byte[]) return new String((byte[]) object);
		return object;
	}

	private String prepareInsertSQL(Event event) {
		return "INSERT INTO " + databaseAndTable(event) + " SET " + fields(event);
	}

	private String prepareUpdateSQL(Event event) {
		return "UPDATE " + databaseAndTable(event) + " SET " + fields(event) + " WHERE " + conditions(event);
	}

	private String prepareDeleteSQL(Event event) {
		return "DELETE FROM " + databaseAndTable(event) + " WHERE " + conditions(event);
	}

	private String databaseAndTable(Event event) {
		return event.getNamespace() + "." + event.getCollection();
	}

	private String fields(Event event) {
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