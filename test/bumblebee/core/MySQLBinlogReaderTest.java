package bumblebee.core;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import bumblebee.core.aux.DummyConsumer;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.SchemaManager;
import bumblebee.core.reader.MySQLBinlogAdapter;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
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
		@Override public String getColumnName(String tableName, int index) {
			return tableSchemas.get(tableName).get(index);
		}
	}

	@Test public void shouldTransformInsertBinlogEventIntoGenericData() throws BusinessException {
		DummyConsumer readerx = new DummyConsumer();
		MySQLBinlogAdapter reader = new MySQLBinlogAdapter();
		reader.attach(readerx);
		reader.setSchemaManager(new DummySchemaManager());

		TableMapEventData tmed = new TableMapEventData();
		tmed.setTable("some_table");
		tmed.setDatabase("some_database");
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

		assertEquals("some_table", readerx.getLastEvent().getCollection());
		assertEquals("some_database", readerx.getLastEvent().getNamespace());
		assertEquals(expectedData, readerx.getLastEvent().getData());
	}

	@Test public void shouldTransformUpdateBinlogEventIntoGenericData() throws BusinessException {
		DummyConsumer readerx = new DummyConsumer();
		MySQLBinlogAdapter reader = new MySQLBinlogAdapter();
		reader.attach(readerx);
		reader.setSchemaManager(new DummySchemaManager());

		TableMapEventData tmed = new TableMapEventData();
		tmed.setTable("some_table");
		tmed.setDatabase("some_database");
		tmed.setTableId(2L);
		reader.mapTable(tmed);

		UpdateRowsEventData ured = new UpdateRowsEventData();
		ured.setTableId(2L);

		Serializable[] rowCond = {"first_col_old_value", "second_col_old_value"};
		Serializable[] rowVal = {"first_col_new_value", "second_col_new_value"};

		List<Map.Entry<Serializable[], Serializable[]>> rowsz = new ArrayList<Map.Entry<Serializable[], Serializable[]>>();
		rowsz.add(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(rowCond, rowVal));

		ured.setRows(rowsz);
		ured.setIncludedColumns(BitSet.valueOf(new long[0]));

		reader.transformUpdate(ured);

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
	}
	
	@Test public void shouldTransformDeleteBinlogEventIntoGenericData() throws BusinessException {
		DummyConsumer readerx = new DummyConsumer();
		MySQLBinlogAdapter reader = new MySQLBinlogAdapter();
		reader.attach(readerx);
		reader.setSchemaManager(new DummySchemaManager());

		TableMapEventData tmed = new TableMapEventData();
		tmed.setTable("some_table");
		tmed.setDatabase("some_database");
		tmed.setTableId(2L);
		reader.mapTable(tmed);

		DeleteRowsEventData dred = new DeleteRowsEventData();
		dred.setTableId(2L);

		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"first_col_value", "second_col_value"};
		rows.add(row);
		dred.setRows(rows);
		dred.setIncludedColumns(BitSet.valueOf(new long[0]));
		reader.transformDelete(dred);

		Map<String, Object> expectedCondition = new HashMap<String, Object>();
		expectedCondition.put("first_col_name", "first_col_value");
		expectedCondition.put("second_col_name", "second_col_value");

		assertEquals("some_table", readerx.getLastEvent().getCollection());
		assertEquals("some_database", readerx.getLastEvent().getNamespace());
		assertEquals(expectedCondition, readerx.getLastEvent().getConditions());
		assertEquals(new Long(12L), readerx.getCurrentLogPosition().getPosition());
	}
	
	@Test public void shouldProcessLogRotateEvent() throws BusinessException {
		DummyConsumer readerx = new DummyConsumer();
		MySQLBinlogAdapter reader = new MySQLBinlogAdapter();
		reader.attach(readerx);
		reader.setSchemaManager(new DummySchemaManager());
		
		RotateEventData red = new RotateEventData();
		red.setBinlogFilename("mysql-binlog.000001");
		red.setBinlogPosition(4L);
		
		reader.changePosition(red);
		
		assertEquals("mysql-binlog.000001", readerx.getCurrentLogPosition().getFilename());
		assertEquals(new Long(4L), readerx.getCurrentLogPosition().getPosition());
	}

}
