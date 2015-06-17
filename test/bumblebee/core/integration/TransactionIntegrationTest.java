package bumblebee.core.integration;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.SchemaManager;
import bumblebee.core.reader.MySQLBinlogAdapter;

import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class TransactionIntegrationTest {

	@Test public void shouldCommitAfterConsumeEvent() throws BusinessException, SQLException {
		Consumer consumer = mock(Consumer.class);
		
		MySQLBinlogAdapter producer = new MySQLBinlogAdapter(consumer, mock(SchemaManager.class));

		WriteRowsEventData wred = new WriteRowsEventData();
		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"first_col_value", "second_col_value"};
		rows.add(row);
		wred.setRows(rows);
		EventHeaderV4 eventHeader = mock(EventHeaderV4.class);

		producer.transformInsert(wred, eventHeader);
		verify(consumer).consume(any());
		verify(consumer).commit();
		verify(consumer, never()).rollback();
	}

	@Test public void shouldRollbackIfAnExceptionIsThrown() throws BusinessException {
		Consumer consumer = mock(Consumer.class);
		doThrow(BusinessException.class).when(consumer).consume(any());
		
		MySQLBinlogAdapter producer = new MySQLBinlogAdapter(consumer, mock(SchemaManager.class));

		WriteRowsEventData wred = new WriteRowsEventData();
		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"first_col_value", "second_col_value"};
		rows.add(row);
		wred.setRows(rows);
		EventHeaderV4 eventHeader = mock(EventHeaderV4.class);

		try {
			producer.transformInsert(wred, eventHeader);
			fail("Exception not thrown.");
		} catch(BusinessException ex) {
			verify(consumer).consume(any());
			verify(consumer).rollback();
			verify(consumer, never()).commit();
		}
	}
}
