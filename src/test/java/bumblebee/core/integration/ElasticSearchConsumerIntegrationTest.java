package bumblebee.core.integration;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import bumblebee.core.applier.ElasticSearchConsumer;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;

public class ElasticSearchConsumerIntegrationTest extends ElasticsearchIntegrationTest {
	private Client client;;

	static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }
	
	@Before public void setup() {
		client = client();
		flushAndRefresh();
	}

	@Test public void shouldGetTheCurrentPosition() {
		String name = "a_name";
		Long position = 4L;

		LogPosition expectedLogPosition = new LogPosition(name, position);

		String source = "{\"logName\":\"" + name + "\",\"logPosition\":" + position + "}";
		IndexRequestBuilder request = client.prepareIndex("consumer_position", "log_position", "1");
		request.setSource(source);
		request.get();

		ElasticSearchConsumer consumer = new ElasticSearchConsumer(client );
		LogPosition currentLogPosition = consumer.getCurrentLogPosition();

		assertEquals(expectedLogPosition.getFilename(), currentLogPosition.getFilename());
		assertEquals(expectedLogPosition.getPosition(), currentLogPosition.getPosition());
	}
	
	@Test(expected = BusinessException.class)
	public void shouldThrowBusinessExceptionIfIndexIsMissing() {
		ElasticSearchConsumer consumer = new ElasticSearchConsumer(client);
		consumer.getCurrentLogPosition();
	}
	
	@Test(expected = BusinessException.class)
	public void shouldThrowBusinessExceptionIfPositionWasntSetButIndexDoExist() {
		String source = "{\"logName\":\"a_name\",\"logPosition\":0}";
		IndexRequestBuilder request = client.prepareIndex("consumer_position", "wrong_type_to_break_the_test", "1");
		request.setSource(source);
		request.get();

		ElasticSearchConsumer consumer = new ElasticSearchConsumer(client);
		consumer.getCurrentLogPosition();
	}
}
