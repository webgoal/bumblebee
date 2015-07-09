package bumblebee.core;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import bumblebee.core.applier.ElasticSearchConsumer;
import bumblebee.core.events.DeleteEvent;
import bumblebee.core.events.InsertEvent;
import bumblebee.core.events.UpdateEvent;

public class ElasticSearchConsumerTest {
	private Map<String, Object> data = new LinkedHashMap<String, Object>();
	private Map<String, Object> conditions = new LinkedHashMap<String, Object>();
	private Client client = mock(Client.class);;
	private ElasticSearchConsumer consumer = new ElasticSearchConsumer(client);;

	@Before public void setup() {
		data.put("id", 1);
		data.put("name", "Someone");
		conditions.put("id", 2);
	}

	@Test public void eventToIndexTest() {
		InsertEvent insertEvent = new InsertEvent();
		insertEvent.setNamespace("namespace");
		insertEvent.setCollection("collection");
		insertEvent.setData(data);
		
		IndexRequestBuilder request = mock(IndexRequestBuilder.class);
		doReturn(request).when(client).prepareIndex(insertEvent.getNamespace(), insertEvent.getCollection(), data.get("id").toString());

		consumer.consume(insertEvent);

		verify(client).prepareIndex(insertEvent.getNamespace(), insertEvent.getCollection(), data.get("id").toString());
		verify(request).setSource(data);
		verify(request).get();
	}

	@Test public void eventToUpdateTest() {
		UpdateEvent updateEvent = new UpdateEvent();
		updateEvent.setNamespace("namespace");
		updateEvent.setCollection("collection");
		updateEvent.setData(data);
		
		UpdateRequestBuilder request = mock(UpdateRequestBuilder.class);
		doReturn(request).when(client).prepareUpdate(updateEvent.getNamespace(), updateEvent.getCollection(), data.get("id").toString());

		consumer.consume(updateEvent);

		verify(client).prepareUpdate(updateEvent.getNamespace(), updateEvent.getCollection(), data.get("id").toString());
		verify(request).setDoc(data);
		verify(request).get();
	}

	@Test public void eventToDeleteTest() {
		DeleteEvent deleteEvent = new DeleteEvent();
		deleteEvent.setNamespace("namespace");
		deleteEvent.setCollection("collection");
		deleteEvent.setData(data);
		
		DeleteRequestBuilder request = mock(DeleteRequestBuilder.class);
		doReturn(request).when(client).prepareDelete(deleteEvent.getNamespace(), deleteEvent.getCollection(), data.get("id").toString());

		consumer.consume(deleteEvent);

		verify(client).prepareDelete(deleteEvent.getNamespace(), deleteEvent.getCollection(), data.get("id").toString());
		verify(request).setId(data.get("id").toString());
		verify(request).get();
	}

	@Test public void setCompletePositionTest() {
		IndexRequestBuilder request = mock(IndexRequestBuilder.class);
		doReturn(request).when(client).prepareIndex("consumer_position", "log_position", "1");
		String name = "name";
		Long position = 4L;

		consumer.setPosition(name, position);

		verify(client).prepareIndex("consumer_position", "log_position", "1");
		verify(request).setSource("{\"logName\":\"" + name + "\",\"logPosition\":" + position + "}");
		verify(request).get();
	}

	@Test public void setOnlyPositionTest() {
		UpdateRequestBuilder request = mock(UpdateRequestBuilder.class);
		doReturn(request).when(client).prepareUpdate("consumer_position", "log_position", "1");
		Long position = 4L;

		consumer.setPosition(position);

		verify(client).prepareUpdate("consumer_position", "log_position", "1");
		verify(request).setDoc("{\"logPosition\": 4}");
		verify(request).get();
	}
}
