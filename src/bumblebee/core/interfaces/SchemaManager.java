package bumblebee.core.interfaces;

import bumblebee.core.exceptions.BusinessException;

public interface SchemaManager {
	String getColumnName(String dbName, String tableName, int index) throws BusinessException;
}
