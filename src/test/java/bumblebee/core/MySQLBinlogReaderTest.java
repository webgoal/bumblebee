package bumblebee.core;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import bumblebee.core.aux.DummyConsumer;
import bumblebee.core.interfaces.SchemaManager;
import bumblebee.core.reader.MySQLBinlogAdapter;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogReaderTest {

	class DummySchemaManager implements SchemaManager {
		private Map<String, List<String>> tableSchemas = new HashMap<String, List<String>>();
		LinkedList<String> cols = new LinkedList<String>();

		public DummySchemaManager() {
			cols.add("first_col_name");
			cols.add("second_col_name");
			tableSchemas.put("some_table", cols);
		}

		@Override public String getColumnName(String dbName, String tableName, int index) {
			return tableSchemas.get(tableName).get(index);
		}
	}

	private DummyConsumer readerx;
	private MySQLBinlogAdapter reader;

	@Before public void setup() {
		readerx = new DummyConsumer();
		readerx.setPosition("mysqllog-name", 0L);
		
		reader = new MySQLBinlogAdapter(readerx, new DummySchemaManager());
	}
	
	private EventHeaderV4 eventHeader() {
		Long nextPosition = 12L;
		EventHeaderV4 eventHeader = new EventHeaderV4();
		eventHeader.setNextPosition(nextPosition);
		return eventHeader;
	}
	
	private TableMapEventData tableMap() {
		TableMapEventData tmed = new TableMapEventData();
		tmed.setTable("some_table");
		tmed.setDatabase("some_database");
		tmed.setTableId(2L);
		return tmed;
	}
	
	@Test public void shouldTransformInsertBinlogEventIntoGenericData() {
		reader.mapTable(tableMap());

		WriteRowsEventData wred = new WriteRowsEventData();
		wred.setTableId(2L);
		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"first_col_value", "second_col_value"};
		rows.add(row);
		wred.setRows(rows);
		
		reader.transformInsert(wred, eventHeader());

		Map<String, Object> expectedData = new HashMap<String, Object>();
		expectedData.put("first_col_name", "first_col_value");
		expectedData.put("second_col_name", "second_col_value");

		assertEquals("some_table", readerx.getLastEvent().getCollection());
		assertEquals("some_database", readerx.getLastEvent().getNamespace());
		assertEquals(expectedData, readerx.getLastEvent().getData());
		assertEquals("mysqllog-name", readerx.getCurrentLogPosition().getFilename());
		assertEquals(new Long(eventHeader().getNextPosition()), readerx.getCurrentLogPosition().getPosition());
	}

	@Test public void shouldTransformUpdateBinlogEventIntoGenericData() {
		reader.mapTable(tableMap());

		UpdateRowsEventData ured = new UpdateRowsEventData();
		ured.setTableId(2L);

		Serializable[] rowCond = {"first_col_old_value", "second_col_old_value"};
		Serializable[] rowVal = {"first_col_new_value", "second_col_new_value"};

		List<Map.Entry<Serializable[], Serializable[]>> rowsz = new ArrayList<Map.Entry<Serializable[], Serializable[]>>();
		rowsz.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(rowCond, rowVal));

		ured.setRows(rowsz);

		reader.transformUpdate(ured, eventHeader());

		Map<String, Object> expectedCondition = new HashMap<String, Object>();
		expectedCondition.put("first_col_name", "first_col_old_value");
		expectedCondition.put("second_col_name", "second_col_old_value");

		Map<String, Object> expectedData = new HashMap<String, Object>();
		expectedData.put("first_col_name", "first_col_new_value");
		expectedData.put("second_col_name", "second_col_new_value");

		assertEquals("some_table", readerx.getLastEvent().getCollection());
		assertEquals(expectedCondition, readerx.getLastEvent().getConditions());
		assertEquals("some_database", readerx.getLastEvent().getNamespace());
		assertEquals(expectedData, readerx.getLastEvent().getData());
		assertEquals("mysqllog-name", readerx.getCurrentLogPosition().getFilename());
		assertEquals(new Long(eventHeader().getNextPosition()), readerx.getCurrentLogPosition().getPosition());
	}
	
	@Test public void shouldTransformDeleteBinlogEventIntoGenericData() {
		reader.mapTable(tableMap());

		DeleteRowsEventData dred = new DeleteRowsEventData();
		dred.setTableId(2L);

		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"first_col_value", "second_col_value"};
		rows.add(row);
		dred.setRows(rows);

		reader.transformDelete(dred, eventHeader());

		Map<String, Object> expectedCondition = new HashMap<String, Object>();
		expectedCondition.put("first_col_name", "first_col_value");
		expectedCondition.put("second_col_name", "second_col_value");

		assertEquals("some_table", readerx.getLastEvent().getCollection());
		assertEquals("some_database", readerx.getLastEvent().getNamespace());
		assertEquals(expectedCondition, readerx.getLastEvent().getConditions());
		assertEquals("mysqllog-name", readerx.getCurrentLogPosition().getFilename());
		assertEquals(new Long(eventHeader().getNextPosition()), readerx.getCurrentLogPosition().getPosition());
	}
	
	@Test public void shouldProcessLogRotateEvent() {
		RotateEventData red = new RotateEventData();
		red.setBinlogFilename("mysql-binlog.000001");
		red.setBinlogPosition(4L);
		
		reader.changePosition(red);
		
		assertEquals("mysql-binlog.000001", readerx.getCurrentLogPosition().getFilename());
		assertEquals(new Long(4L), readerx.getCurrentLogPosition().getPosition());
	}
}
