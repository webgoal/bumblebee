package bumblebee.samples.elastic_application;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class Main {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		//Create Client
		Client client = null;
		try {
			Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
			TransportAddress transportAddress = new TransportAddress(InetAddress.getByName("192.168.99.100"), 9200);
			TransportClient transportClient = new PreBuiltTransportClient(settings).addTransportAddress(transportAddress);
			client = (Client) transportClient;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		//Create Index and set settings and mappings
		
		DeleteIndexRequestBuilder d = client.admin().indices().prepareDelete("test");
		d.execute().actionGet();

//		CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate("test");
//		createIndexRequestBuilder.execute().actionGet();

		//Add documents
		IndexRequestBuilder indexRequestBuilder = client.prepareIndex("test", "licit", "1");
		//build json object
		XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject().prettyPrint();
		contentBuilder.field("name", "jasdasdasdasdasdai");
		contentBuilder.endObject();
		Map <String, Object> data = new LinkedHashMap<String, Object>();
		data.put("name", "tots");
		data.put("id", 10);
		indexRequestBuilder.setSource(data).get();
		//Get document
//		GetRequestBuilder getRequestBuilder = client.prepareGet("test", "licit", "1");
//		getRequestBuilder.setFields(new String[]{"name"});
	}

}