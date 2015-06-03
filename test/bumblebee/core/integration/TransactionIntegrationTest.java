package bumblebee.core.integration;

import static org.mockito.Mockito.*;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import bumblebee.core.applier.MySQLConsumer;
import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.SchemaManager;
import bumblebee.core.interfaces.Transformer;
import bumblebee.core.reader.MySQLBinlogAdapter;
import bumblebee.transformer.DelegateTransformer;

public class TransactionIntegrationTest extends SQLIntegrationTestBase {

	@Test public void shouldCommitAfterConsumeEvent() throws BusinessException, SQLException {
		MySQLBinlogAdapter producer = new MySQLBinlogAdapter();
		producer.setSchemaManager(mock(SchemaManager.class));
		
		Transformer transformer = new DelegateTransformer();
		producer.attach(transformer);
		
		Consumer mySQLConsumer = new MySQLConsumer();
		mySQLConsumer.setPositionManager(mock(MySQLPositionManager.class));
		transformer.attach(mySQLConsumer);
		
		TableMapEventData tmed = new TableMapEventData();
		tmed.setTable("some_table");
		tmed.setDatabase("some_database");
		tmed.setTableId(2L);
		producer.mapTable(tmed);

		WriteRowsEventData wred = new WriteRowsEventData();
		wred.setTableId(2L);
		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"first_col_value", "second_col_value"};
		rows.add(row);
		wred.setRows(rows);
		
		Long nextPosition = 12L;
		EventHeaderV4 eventHeader = new EventHeaderV4();
		eventHeader.setNextPosition(nextPosition);
		
		producer.transformInsert(wred, eventHeader);
	}
}
