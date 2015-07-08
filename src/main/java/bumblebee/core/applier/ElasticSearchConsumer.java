package bumblebee.core.applier;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;

public class ElasticSearchConsumer extends RESTConsumer {
	private Client elasticSearchClient;

	public ElasticSearchConsumer(Client elasticSearchClient) {
		this.elasticSearchClient = elasticSearchClient;
	}

	@Override public void setPosition(String logName, long logPosition) {
		// TODO Auto-generated method stub
		
	}

	@Override public void setPosition(long logPosition) {
		// TODO Auto-generated method stub
		
	}

	@Override public LogPosition getCurrentLogPosition() {
		// TODO Auto-generated method stub
		return null;
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
