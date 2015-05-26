package bumblebee.core.reader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import bumblebee.core.Event;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogReader {
	private EventListener reader;
	private Map<Long, String> tableInfo = new HashMap<Long, String>();
	private Map<String, List<String>> tableSchemas = new HashMap<String, List<String>>();
	
	public MySQLBinlogReader(EventListener reader) {
		this.reader = reader;
		LinkedList<String> cols = new LinkedList<String>();
		cols.add("first_col_name");
		cols.add("second_col_name");
		tableSchemas.put("some_table", cols);
	}

	public String getTableById(Long tableId) {
		return tableInfo.get(tableId);
	}
	
	public void mapTable(TableMapEventData data) {
		tableInfo.put(data.getTableId(), data.getTable());
	}

	public void transformInsert(WriteRowsEventData data) {
//		Iterator<Serializable[]> it = data.getRows().iterator();
//		Serializable[] s = it.next();

		for (Serializable[] row : data.getRows()) {
			Event event = new Event();
			event.setCollection(tableInfo.get(data.getTableId()));			
			
			event.setData(dataToMap(tableInfo.get(data.getTableId()), row));
			
			reader.onInsert(event);
		}
	}

	private Map<String, Object> dataToMap(String tableName, Serializable[] row) {
		Map<String, Object> map = new HashMap<String, Object>();
		int i = 0;
		for (Serializable value : row)
			map.put(tableSchemas.get(tableName).get(i++), value);
		return map;
	}

	
}
