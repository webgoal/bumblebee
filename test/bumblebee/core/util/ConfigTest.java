package bumblebee.core.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConfigTest {

	@Test public void testLoadConsumerConfig() {
		Config config = ConfigFactory.load("bumblebee/core/util/application.conf");
		MySQLConnectionManager.configLoader(config);
		
		assertEquals("192.168.0.1", MySQLConnectionManager.getConsumerHost());
		assertEquals(3306, MySQLConnectionManager.getConsumerPort());
		assertEquals("guest", MySQLConnectionManager.getConsumerUser());
        assertEquals("some", MySQLConnectionManager.getConsumerPass());
	}

	@Test public void testLoadProducerConfig() {
		Config config = ConfigFactory.load("bumblebee/core/util/application.conf");
		MySQLConnectionManager.configLoader(config);
		
		assertEquals("192.168.0.2", MySQLConnectionManager.getProducerHost());
		assertEquals(3307, MySQLConnectionManager.getProducerPort());
		assertEquals("root", MySQLConnectionManager.getProducerUser());
        assertEquals("pass", MySQLConnectionManager.getProducerPass());
	}

}
