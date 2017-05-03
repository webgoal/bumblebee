package bumblebee.core.integration;

import static org.junit.Assert.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.reader.MySQLSchemaManager;

public class MySQLSchemaManagerIntegrationTest extends SQLIntegrationTestBase {
	
	@Rule public ExpectedException assertThrown = ExpectedException.none();

	@Test public void shouldReadData() {
		MySQLSchemaManager schemaManager = new MySQLSchemaManager(getFakeConnection());
		assertEquals("id", schemaManager.getColumnName("db1", "test", 0));
		assertEquals("name", schemaManager.getColumnName("db1", "test", 1));
	}
	
	@Test public void shouldCacheLoadedData() {
		MySQLSchemaManager schemaManager = new MySQLSchemaManager(getFakeConnection());
		schemaManager.getColumnName("db1", "test", 0);
		runSQL("ALTER TABLE test DROP COLUMN name;");
		assertEquals("name", schemaManager.getColumnName("db1", "test", 1));
	}
	
	@Test public void shouldHonorSchemaName() {
		MySQLSchemaManager schemaManager = new MySQLSchemaManager(getFakeConnection());
		
		assertThrown.expect(BusinessException.class);
		schemaManager.getColumnName("db2", "test", 0);
	}
}
