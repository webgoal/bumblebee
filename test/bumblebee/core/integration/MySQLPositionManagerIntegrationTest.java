package bumblebee.core.integration;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.Test;

import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.exceptions.BusinessException;

public class MySQLPositionManagerIntegrationTest extends SQLIntegrationTestBase {
	
	@Test public void shouldReturnInitialPosition() throws BusinessException {
		String expectedLogName = "mysql-bin.000001";
		Long expectedLogPosition = 4L;
		MySQLPositionManager positionManager = new MySQLPositionManager("", "log_position");
		positionManager.setConnection(getFakeConnection());
		
		assertEquals(expectedLogName, positionManager.getCurrentLogPosition().getFilename());
		assertEquals(expectedLogPosition, positionManager.getCurrentLogPosition().getPosition());
	}
	
	@Test public void shouldUpdateCurrentPosition() throws BusinessException {
		String expectedLogName = "mysql-bin.000002";
		Long expectedLogPosition = 10L;
		MySQLPositionManager positionManager = new MySQLPositionManager("", "log_position");
		positionManager.setConnection(getFakeConnection());
		
		positionManager.update(expectedLogName, expectedLogPosition);
		
		assertEquals(expectedLogName, positionManager.getCurrentLogPosition().getFilename());
		assertEquals(expectedLogPosition, positionManager.getCurrentLogPosition().getPosition());
	}
	
	@Test public void shouldThrowExceptionOnControlTableMissing() throws BusinessException {
		MySQLPositionManager positionManager = new MySQLPositionManager("", "inexistent_log_position_table");
		positionManager.setConnection(getFakeConnection());
		
		assertThrown.expect(BusinessException.class);
		positionManager.getCurrentLogPosition();
	}
	
	@Test public void shouldThrowExceptionOnControlTableEmpty() throws BusinessException, SQLException {
		MySQLPositionManager positionManager = new MySQLPositionManager("", "log_position");
		positionManager.setConnection(getFakeConnection());
		
		connection.createStatement().executeUpdate("TRUNCATE TABLE log_position");
		
		assertThrown.expect(BusinessException.class);
		positionManager.getCurrentLogPosition();
	}
	
}
