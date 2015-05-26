package bumblebee.core.reader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import bumblebee.core.Event;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogReader {
	private EventListener reader;
	private Map<Long, String> tableInfo = new HashMap<Long, String>();
	private SchemaManager schemaManager;
	
	public MySQLBinlogReader(EventListener reader) {
		this.reader = reader;
	}

	public String getTableById(Long tableId) {
		return tableInfo.get(tableId);
	}
	
	public void setSchemaManager(SchemaManager schemaManager) {
		this.schemaManager = schemaManager;
	}
	
	public void mapTable(TableMapEventData data) {
		tableInfo.put(data.getTableId(), data.getTable());
	}

	public void transformInsert(WriteRowsEventData data) {
		for (Serializable[] row : data.getRows()) {
			Event event = new Event();
			event.setCollection(tableInfo.get(data.getTableId()));			
			event.setData(dataToMap(tableInfo.get(data.getTableId()), row));
			
			reader.onInsert(event);
		}
	}

	private Map<String, Object> dataToMap(String tableName, Serializable[] row) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < row.length; i++)
			map.put(schemaManager.getColumnName(tableName, i), row[i]);
		return map;
	}
	
}
