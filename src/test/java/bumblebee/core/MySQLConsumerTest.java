package bumblebee.core;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import bumblebee.core.applier.MySQLConsumer;
import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.events.DeleteEvent;
import bumblebee.core.events.InsertEvent;
import bumblebee.core.events.UpdateEvent;

public class MySQLConsumerTest {
	
	private Map<String, Object> data = new HashMap<String, Object>();
	private Map<String, Object> conditions = new HashMap<String, Object>();
	
	@Before public void setup() {
		data.put("name", "Someone");
		data.put("id", 1);
		conditions.put("id", 2);
	}

	@Test public void eventToSQLInsertTransformationTest() throws SQLException {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		doReturn(statement).when(connection).prepareStatement(any());
		
		MySQLConsumer consumer = new MySQLConsumer(connection, mock(MySQLPositionManager.class));
		
		InsertEvent insertEvent = new InsertEvent();
		insertEvent.setNamespace("database");
		insertEvent.setCollection("table");
		insertEvent.setData(data);
		
		consumer.consume(insertEvent);
		verify(connection).prepareStatement("INSERT INTO database.table SET name = ?, id = ?");
		verify(statement).setObject(1, "Someone");
		verify(statement).setObject(2, 1);
		verify(statement).executeUpdate();
	}
	
	@Test public void eventToSQLUpdateTransformationTest() throws SQLException {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		doReturn(statement).when(connection).prepareStatement(any());
		
		MySQLConsumer consumer = new MySQLConsumer(connection, mock(MySQLPositionManager.class));
		
		UpdateEvent updateEvent = new UpdateEvent();
		updateEvent.setNamespace("database");
		updateEvent.setCollection("table");
		updateEvent.setData(data);
		updateEvent.setConditions(conditions);
		
		consumer.consume(updateEvent);
		verify(connection).prepareStatement("UPDATE database.table SET name = ?, id = ? WHERE id = ?");
		verify(statement).setObject(1, "Someone");
		verify(statement).setObject(2, 1);
		verify(statement).setObject(3, 2);
		verify(statement).executeUpdate();
	}

	@Test public void eventToSQLDeleteTransformationTest() throws SQLException {
		Connection connection = mock(Connection.class);
		PreparedStatement statement = mock(PreparedStatement.class);
		doReturn(statement).when(connection).prepareStatement(any());
		
		MySQLConsumer consumer = new MySQLConsumer(connection, mock(MySQLPositionManager.class));
		
		DeleteEvent deleteEvent = new DeleteEvent();
		deleteEvent.setNamespace("database");
		deleteEvent.setCollection("table");
		deleteEvent.setConditions(conditions);
		
		consumer.consume(deleteEvent);
		verify(connection).prepareStatement("DELETE FROM database.table WHERE id = ?");
		verify(statement).setObject(1, 2);
		verify(statement).executeUpdate();
	}

}
