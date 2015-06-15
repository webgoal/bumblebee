package bumblebee.core.reader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.SchemaManager;

public class MySQLSchemaManager implements SchemaManager {
	private Map<String, List<String>> tableSchemas = new HashMap<String, List<String>>();
	private Connection connection;

	public MySQLSchemaManager(Connection connection) {
		this.connection = connection;
	}
	
	@Override public String getColumnName(String dbName, String tableName, int index) throws BusinessException {
		if (!tableSchemas.containsKey(tableName))
			loadTableSchema(dbName, tableName);
		try {
			return tableSchemas.get(tableName).get(index);
		} catch (IndexOutOfBoundsException ex) {
			throw new BusinessException(ex);
		}
	}

	private void loadTableSchema(String dbName, String tableName) {
		try {
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet columnsMetaData = meta.getColumns(dbName, null, tableName, null);
			
			List<String> columns = new LinkedList<String>();
			while(columnsMetaData.next()) {
				String name = columnsMetaData.getString("COLUMN_NAME");
				columns.add(name);
			}
			this.tableSchemas.put(tableName, columns);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
