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
	
	@Override public String getColumnName(String dbName, String tableName, int index) {
		String fullTableName = dbName + "/" + tableName;
		if (!tableSchemas.containsKey(fullTableName))
			tableSchemas.put(fullTableName, loadTableSchema(dbName, tableName));
		try {
			return tableSchemas.get(fullTableName).get(index);
		} catch (IndexOutOfBoundsException ex) {
			throw new BusinessException(ex);
		}
	}

	private List<String> loadTableSchema(String dbName, String tableName) {
		List<String> columns = new LinkedList<String>();
		try {
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet columnsMetaData = meta.getColumns(dbName, null, tableName, null);
			while(columnsMetaData.next())
				columns.add(columnsMetaData.getString("COLUMN_NAME"));
			columnsMetaData.close();
		} catch (Exception ex) {
			throw new BusinessException(ex);
		}
		return columns;
	}
}
