package bumblebee.core.reader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import bumblebee.core.Event;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.MySQLSchemaManager;
import bumblebee.core.interfaces.Producer;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogAdapter implements Producer {
	private Consumer consumer;
	
	private Map<Long, String> tableInfo = new HashMap<Long, String>();
	private MySQLSchemaManager schemaManager;
	
	public String getTableById(Long tableId) {
		return tableInfo.get(tableId);
	}
	
	public void setSchemaManager(MySQLSchemaManager schemaManager) {
		this.schemaManager = schemaManager;
	}
	
	@Override public void attach(Consumer consumer) {
		this.consumer = consumer;
	}

	public void mapTable(TableMapEventData data) {
		tableInfo.put(data.getTableId(), data.getTable());
	}

	public void transformInsert(WriteRowsEventData data) {
		for (Serializable[] row : data.getRows()) {
			Event event = new Event();
			event.setCollection(tableInfo.get(data.getTableId()));			
			event.setData(dataToMap(tableInfo.get(data.getTableId()), row));
			
			consumer.insert(event);
		}
	}

	private Map<String, Object> dataToMap(String tableName, Serializable[] row) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < row.length; i++)
			map.put(schemaManager.getColumnName(tableName, i), row[i]);
		return map;
	}
	
}
