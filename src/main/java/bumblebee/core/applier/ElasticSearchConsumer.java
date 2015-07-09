package bumblebee.core.applier;

import java.io.IOException;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexMissingException;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;

public class ElasticSearchConsumer extends RESTConsumer {
	private Client elasticSearchClient;

	public ElasticSearchConsumer(Client elasticSearchClient) {
		this.elasticSearchClient = elasticSearchClient;
	}

	@Override public void setPosition(String logName, long logPosition) {
		IndexRequestBuilder request = elasticSearchClient.prepareIndex("consumer_position", "log_position", "1");

		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();
			builder.field("logName", logName);
			builder.field("logPosition", logPosition);
			builder.endObject();
			request.setSource(builder.string());
			request.get();
		} catch (IOException e) {
			throw new BusinessException(e);
		}
	}

	@Override public void setPosition(long logPosition) {
		UpdateRequestBuilder request = elasticSearchClient.prepareUpdate("consumer_position", "log_position", "1");
		request.setDoc("{\"logPosition\": "+ logPosition +"}");
		request.get();
	}

	@Override public LogPosition getCurrentLogPosition() {
		try {
			GetResponse response = elasticSearchClient.prepareGet("consumer_position", "log_position", "1").execute().actionGet();
			return new LogPosition(response.getSource().get("logName").toString(), Long.parseLong(response.getSource().get("logPosition").toString()));
		} catch (IndexMissingException | NullPointerException e) {
			throw new BusinessException(e);
		}
	}

	@Override protected void insert(Event event) {
		IndexRequestBuilder request = elasticSearchClient.prepareIndex(event.getNamespace(), event.getCollection(), event.getData().get("id").toString());
		request.setSource(event.getData());
		request.get();
	}

	@Override protected void update(Event event) {
		UpdateRequestBuilder request = elasticSearchClient.prepareUpdate(event.getNamespace(), event.getCollection(), event.getData().get("id").toString());
		request.setDoc(event.getData());
		request.get();
	}

	@Override protected void delete(Event event) {
		DeleteRequestBuilder request = elasticSearchClient.prepareDelete(event.getNamespace(), event.getCollection(), event.getData().get("id").toString());
		request.setId(event.getData().get("id").toString());
		request.get();
	}

}
