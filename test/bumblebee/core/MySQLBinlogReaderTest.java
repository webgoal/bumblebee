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

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogReaderTest {
	
	class DummyListener implements EventListener {
		public Event lastEvent;
		@Override public void onInsert(Event event) {
			this.lastEvent = event;
		}
	}

	@Test public void shouldTransformBinlogEventIntoGenericData() {
		DummyListener readerx = new DummyListener();
		MySQLBinlogReader reader = new MySQLBinlogReader(readerx);
		
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
	
	
//	  private def onInsert(header: EventHeaderV4, data: WriteRowsEventData) {
//	    val np = header.getNextPosition
//	    val id = data.getTableId
//	    val ti = tablesById.get(id)
//
//	    if (doesDbNeedRep(ti, np)) synchronized {
//	      for (listener <- listeners) {
//	        listener.onEvent(new RepEvent.Insert(np, ti.get, data))
//	      }
//	    }
//	  }
}
