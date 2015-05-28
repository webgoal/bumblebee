package bumblebee.core.applier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import bumblebee.core.Event;
import bumblebee.core.interfaces.Consumer;

public class MySQLConsumer implements Consumer {
	private static final String host     = "192.168.59.103";
	private static final String user     = "root";
	private static final String pass     = "mypass";
	private static final String database = "db1";
	private static final Integer port    = 3306;
	private final String url;
	
	public MySQLConsumer() {
		this.url = "jdbc:mysql://" + host + ":" + port + "/" + database;
	}

	@Override public void insert(Event event) {
		executeSql(prepareInsertSQL(event));
	}
	
	@Override public void update(Event event) {
		executeSql(transformEventIntoUpdate(event));
	}

	@Override public void delete(Event event) {
		executeSql(transformEventIntoDelete(event));
	}

	private void executeSql(String sql) {
		try {
			Connection connection  = DriverManager.getConnection(url, user, pass);
			Statement stmt = connection.createStatement();
			stmt.executeUpdate(sql);
		} catch (Exception e) {
			e.printStackTrace();
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
		return serializeMap(event.getData());
	}

	private String conditions(Event event) {
		return serializeMap(event.getConditions());
	}

	private String serializeMap(Map<String, Object> data) {
		StringBuffer sb = new StringBuffer();
		data.forEach((k,v) -> sb.append(k + " = '" + v + "', "));
		sb.replace(sb.length() - 2, sb.length(), "");
		return sb.toString();
	}
}
