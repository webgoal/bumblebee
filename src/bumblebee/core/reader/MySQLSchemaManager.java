package bumblebee.core.reader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import util.ConnectionManager;
import util.MySQLConnectionManager;
import bumblebee.core.interfaces.SchemaManager;

public class MySQLSchemaManager implements SchemaManager {
	private Map<String, List<String>> tableSchemas = new HashMap<String, List<String>>();
	private ConnectionManager connectionManager;

	@Override public String getColumnName(String tableName, int index) {
		if(!tableSchemas.containsKey(tableName)) {
			loadTableSchema(tableName);
			System.out.println(tableSchemas);
		}
		return tableSchemas.get(tableName).get(index);
	}

	private void loadTableSchema(String tableName) {
		try {
			Connection connection  = connectionManager.getProducerConnection();
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet columnsMetaData = meta.getColumns(null, "some_db", tableName, null);
			
			List<String> columns = new LinkedList<String>();
			System.out.println(columnsMetaData);
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

	@Override
	public void setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

}
