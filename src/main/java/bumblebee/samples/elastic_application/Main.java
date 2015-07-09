package bumblebee.samples.elastic_application;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class Main {

	private static TransportClient transportClient;

	public static void main(String[] args) throws IOException {
		//Create Client
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		transportClient = new TransportClient(settings);
		transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		Client client = (Client) transportClient;

		//Create Index and set settings and mappings
		
		DeleteIndexRequestBuilder d = client.admin().indices().prepareDelete("test");
		d.execute().actionGet();

//		CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate("test");
//		createIndexRequestBuilder.execute().actionGet();

		//Add documents
		IndexRequestBuilder indexRequestBuilder = client.prepareIndex("test", "licit", "1");
		//build json object
		XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject().prettyPrint();
		contentBuilder.field("name", "jai");
		contentBuilder.endObject();
		Map <String, Object> data = new LinkedHashMap<String, Object>();
		data.put("name", "tots");
		data.put("id", 10);
		indexRequestBuilder.setSource(data);
		//Get document
		GetRequestBuilder getRequestBuilder = client.prepareGet("test", "licit", "1");
		getRequestBuilder.setFields(new String[]{"name"});
	}

}
