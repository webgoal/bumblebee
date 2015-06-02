package bumblebee.core.interfaces;

import util.ConnectionManager;

public interface SchemaManager {

	String getColumnName(String tableName, int index);
	void setConnectionManager(ConnectionManager connectionManager);
}
