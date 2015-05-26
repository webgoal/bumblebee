package bumblebee.core;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import bumblebee.core.reader.MySQLBinlogReader;
import bumblebee.core.reader.EventListener;
import bumblebee.core.reader.SchemaManager;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogReaderTest {
	
	class DummyListener implements EventListener {
		public Event lastEvent;
		@Override public void onInsert(Event event) {
			this.lastEvent = event;
		}
	}
	
	class DummySchemaManager implements SchemaManager {
		private Map<String, List<String>> tableSchemas = new HashMap<String, List<String>>();
		LinkedList<String> cols = new LinkedList<String>();		
		public DummySchemaManager() {
			cols.add("first_col_name");
			cols.add("second_col_name");
			tableSchemas.put("some_table", cols);
		}
		@Override public String getColumnName(String tableName, int index) {
			return tableSchemas.get(tableName).get(index);
		}
	}

	@Test public void shouldTransformBinlogEventIntoGenericData() {
		DummyListener readerx = new DummyListener();
		MySQLBinlogReader reader = new MySQLBinlogReader(readerx);
		reader.setSchemaManager(new DummySchemaManager());
		
		TableMapEventData tmed = new TableMapEventData();
		tmed.setTable("some_table");
//		tmed.setDatabase("some_database");
		tmed.setTableId(2L);
		reader.mapTable(tmed);

		WriteRowsEventData wred = new WriteRowsEventData();
		wred.setTableId(2L);
		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"first_col_value", "second_col_value"};
		rows.add(row);
		wred.setRows(rows);
		wred.setIncludedColumns(BitSet.valueOf(new long[0]));
		reader.transformInsert(wred);

		Map<String, Object> expectedData = new HashMap<String, Object>();
		expectedData.put("first_col_name", "first_col_value");
		expectedData.put("second_col_name", "second_col_value");
		
		assertEquals("some_table", readerx.lastEvent.getCollection());
		assertEquals(expectedData, readerx.lastEvent.getData());
	}
	
}
