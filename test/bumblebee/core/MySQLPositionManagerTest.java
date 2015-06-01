package bumblebee.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import bumblebee.core.applier.MySQLPositionManager;


public class MySQLPositionManagerTest {
	
	@Test public void shouldReturnCurrentPosition() {
		String expectedLogName = "mysql-bin.000001";
		Long expectedLogPosition = 4L;
		MySQLPositionManager positionManager = new MySQLPositionManager("db_name", "table_name");
		assertEquals(expectedLogName, positionManager.getCurrentLogName());
		assertEquals(expectedLogPosition, positionManager.getCurrentLogPosition());
	}
	
	@Test public void shouldTranslateCurrentPositionIntoSQL() {
		String expectedLogName = "mysql-bin.000001";
		Long expectedLogPosition = 4L;
		MySQLPositionManager positionManager = new MySQLPositionManager("db_name", "table_name");
		String expectedSQL = "UPDATE db_name.table_name SET log_name = 'mysql-bin.000001' SET log_pos = '4'";
		assertEquals(expectedSQL, positionManager.prepareUpdateSQL(expectedLogName, expectedLogPosition));
	}
	
//	Teste quando não há posição
//	Teste quando não há tabela

}
