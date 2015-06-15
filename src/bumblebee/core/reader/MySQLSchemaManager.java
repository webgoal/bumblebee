package bumblebee.core.reader;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import bumblebee.core.interfaces.SchemaManager;
import bumblebee.core.util.MySQLConnectionManager;

public class MySQLSchemaManager implements SchemaManager {
	private Map<String, List<String>> tableSchemas = new HashMap<String, List<String>>();

	@Override public String getColumnName(String tableName, int index) {
		if(!tableSchemas.containsKey(tableName)) {
			loadTableSchema(tableName);
		}
		return tableSchemas.get(tableName).get(index);
	}

	private void loadTableSchema(String tableName) {
		try {
			Connection connection  = MySQLConnectionManager.getProducerConnection();
			DatabaseMetaData meta = connection.getMetaData();
			ResultSet columnsMetaData = meta.getColumns(null, "some_db", tableName, null);
			
			List<String> columns = new LinkedList<String>();
			System.out.println(columnsMetaData);
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
