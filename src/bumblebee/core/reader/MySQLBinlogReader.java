package bumblebee.core.reader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogReader implements Reader {
	private Map<Long, String> tableInfo = new HashMap<Long, String>();
	
	public MySQLBinlogReader() {
		tableInfo.put(1L, "Alt");
	}

	@Override
	public void onInsert(Map<String, Object> data) {
		
	}

	public Map<String, Object> transformInsert(WriteRowsEventData data) {
		Iterator<Serializable[]> it = data.getRows().iterator();
		Serializable[] s = it.next();

		Map<String, Object> genericData = new HashMap<String, Object>();
		genericData.put(tableInfo.get(data.getTableId()), s);
		
		return genericData;
	}

	public String getTableById(Long tableId) {
		return tableInfo.get(tableId);
	}

	public void mapTable(TableMapEventData data) {
		tableInfo.put(data.getTableId(), data.getTable());
	}
}
