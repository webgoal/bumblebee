package bumblebee.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.integration.SQLIntegrationTestBase;


public class MySQLPositionManagerTest extends SQLIntegrationTestBase {
	
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
	
//	Teste quando não há posição
//	Teste quando não há tabela

}
