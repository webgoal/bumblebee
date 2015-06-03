package bumblebee.core.integration;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import bumblebee.core.applier.MySQLConsumer;
import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.aux.H2ConnectionManager;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.SchemaManager;
import bumblebee.core.interfaces.Transformer;
import bumblebee.core.reader.MySQLBinlogAdapter;
import bumblebee.core.reader.MySQLSchemaManager;
import bumblebee.transformer.DelegateTransformer;

import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class TransactionIntegrationTest extends SQLIntegrationTestBase {
	
	private TableMapEventData tableMap() {
		TableMapEventData tmed = new TableMapEventData();
		tmed.setTable("some_table");
		tmed.setDatabase("some_database");
		tmed.setTableId(2L);
		return tmed;
	}

	@Test public void shouldCommitAfterConsumeEvent() throws BusinessException, SQLException {
		MySQLPositionManager positionManager = new MySQLPositionManager("db", "log_position");
		positionManager.setConnection(getFakeConnection());
		
		Connection con = DriverManager.getConnection("jdbc:h2:mem:;MODE=MySQL");
		Statement stmt = con.createStatement();
		stmt.executeUpdate("CREATE SCHEMA some_database");
		stmt.executeUpdate("CREATE TABLE some_database.some_table(first_col varchar(30), second_col varchar(30))");

		Consumer consumer = new MySQLConsumer();
		consumer.setConnectionManager(new H2ConnectionManager());
		consumer.setPositionManager(positionManager);

		Transformer tr = new DelegateTransformer();

		tr.attach(consumer);

		MySQLBinlogAdapter producer = spy(MySQLBinlogAdapter.class);
		SchemaManager sm = new MySQLSchemaManager();
		sm.setConnectionManager(new H2ConnectionManager());
		producer.setSchemaManager(sm);
		producer.attach(tr);

		WriteRowsEventData wred = new WriteRowsEventData();
		wred.setTableId(2L);
		List<Serializable[]> rows = new LinkedList<Serializable[]>();
		Serializable[] row = {"first_col_value", "second_col_value"};
		rows.add(row);
		wred.setRows(rows);
		EventHeaderV4 header = new EventHeaderV4();
		header.setNextPosition(10L);
		
		producer.mapTable(tableMap());
		
		producer.transformInsert(wred, header);
		verify(producer).commit();
	}
}
