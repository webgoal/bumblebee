package bumblebee.core;

import static org.junit.Assert.*;

import java.sql.SQLException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.integration.SQLIntegrationTestBase;


public class MySQLPositionManagerTest extends SQLIntegrationTestBase {
	@Rule public ExpectedException assertThrown = ExpectedException.none();
	
	@Test public void shouldReturnInitialPosition() throws BusinessException {
		String expectedLogName = "mysql-bin.000001";
		Long expectedLogPosition = 4L;
		MySQLPositionManager positionManager = new MySQLPositionManager("db", "log_position");
		positionManager.setConnection(getFakeConnection());
		
		assertEquals(expectedLogName, positionManager.getCurrentLogPosition().getFilename());
		assertEquals(expectedLogPosition, positionManager.getCurrentLogPosition().getPosition());
	}
	
	@Test public void shouldUpdateCurrentPosition() throws BusinessException {
		String expectedLogName = "mysql-bin.000002";
		Long expectedLogPosition = 10L;
		MySQLPositionManager positionManager = new MySQLPositionManager("db", "log_position");
		positionManager.setConnection(getFakeConnection());
		
		positionManager.update(expectedLogName, expectedLogPosition);
		
		assertEquals(expectedLogName, positionManager.getCurrentLogPosition().getFilename());
		assertEquals(expectedLogPosition, positionManager.getCurrentLogPosition().getPosition());
	}
	
	@Test public void shouldThrowExceptionOnControlTableMissing() throws BusinessException {
		MySQLPositionManager positionManager = new MySQLPositionManager("db", "inexistent_log_position_table");
		positionManager.setConnection(getFakeConnection());
		
		assertThrown.expect(BusinessException.class);
		positionManager.getCurrentLogPosition();
	}
	
	@Test public void shouldThrowExceptionOnControlTableEmpty() throws BusinessException, SQLException {
		MySQLPositionManager positionManager = new MySQLPositionManager("db", "log_position");
		positionManager.setConnection(getFakeConnection());
		
		connection.createStatement().executeUpdate("TRUNCATE TABLE db.log_position");
		
		assertThrown.expect(BusinessException.class);
		positionManager.getCurrentLogPosition();
	}
	
}
