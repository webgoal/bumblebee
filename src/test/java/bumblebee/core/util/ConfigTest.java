package bumblebee.core.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ConfigTest {

	@Test public void testLoadConsumerConfig() {
		MySQLConnectionManager.loadConfig("bumblebee/core/util/application-test-core.conf");
		
		assertEquals("192.168.0.1", MySQLConnectionManager.getConsumerHost());
		assertEquals(3306, MySQLConnectionManager.getConsumerPort());
		assertEquals("guest", MySQLConnectionManager.getConsumerUser());
        assertEquals("some", MySQLConnectionManager.getConsumerPass());
        assertEquals(15000, MySQLConnectionManager.getConsumerTimeout());
	}

	@Test public void testLoadProducerConfig() {
		MySQLConnectionManager.loadConfig("bumblebee/core/util/application-test-core.conf");
		
		assertEquals("192.168.0.2", MySQLConnectionManager.getProducerHost());
		assertEquals(3307, MySQLConnectionManager.getProducerPort());
		assertEquals("root", MySQLConnectionManager.getProducerUser());
        assertEquals("pass", MySQLConnectionManager.getProducerPass());
        assertEquals(15000, MySQLConnectionManager.getProducerTimeout());        
	}

}
