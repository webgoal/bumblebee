package bumblebee.core.reader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import bumblebee.core.interfaces.SchemaManager;

public class MySQLSchemaManager implements SchemaManager {
	private Map<String, List<String>> tableSchemas = new HashMap<String, List<String>>();
	private static final String host     = "192.168.59.103";
	private static final String user     = "root";
	private static final String pass     = "mypass";
	private static final String database = "db";
	private static final Integer port    = 3306;

	@Override public String getColumnName(String tableName, int index) {
		if(!tableSchemas.containsKey(tableName))
			loadTableSchema(tableName);
		return tableSchemas.get(tableName).get(index);
	}

	private void loadTableSchema(String tableName) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url  = "jdbc:mysql://" + host + ":" + port + "/" + database;
			Connection connection  = DriverManager.getConnection(url, user, pass);
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet columnsMetaData = meta.getColumns(null, null, tableName, null);
			
			List<String> columns = new LinkedList<String>();
			while(columnsMetaData.next()) {
				String name = columnsMetaData.getString("COLUMN_NAME");
//			String typeLowerCase = columnsMetaData.getString("TYPE_NAME").toLowerCase();
				columns.add(name);
			}
			this.tableSchemas.put(tableName, columns);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
