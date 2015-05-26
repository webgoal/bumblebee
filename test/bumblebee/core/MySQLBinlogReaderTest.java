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

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogReaderTest {

	@Test public void shouldTransformBinlogEventIntoGenericData() {
		MySQLBinlogReader reader = new MySQLBinlogReader();

		Map<String, Object> expectedData = new HashMap<String, Object>();

		WriteRowsEventData wred = new WriteRowsEventData();
		wred.setTableId(1L);
		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"Bixa"};
		rows.add(row);
		wred.setRows(rows);
		wred.setIncludedColumns(BitSet.valueOf(new long[0]));

		expectedData.put("Alt", row);

		assertEquals(expectedData, reader.transformInsert(wred));
	}
	
	@Test public void shouldMapTableIdToTableName() {
		MySQLBinlogReader reader = new MySQLBinlogReader();
		
		TableMapEventData tmed = new TableMapEventData();
		tmed.setTable("foo");
//		tmed.setDatabase("bar");
		tmed.setTableId(2L);
		
		reader.mapTable(tmed);
		assertEquals("foo", reader.getTableById(2L));
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
