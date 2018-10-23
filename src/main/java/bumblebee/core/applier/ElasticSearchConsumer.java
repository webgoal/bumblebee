package bumblebee.core.applier;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.net.MalformedURLException;
import java.io.*;

import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;

public class ElasticSearchConsumer extends RESTConsumer {
	private Logger logger;

	public ElasticSearchConsumer() {
		logger = Logger.getLogger(getClass().getName());
	}

	@Override public void setPosition(String logName, long logPosition) {
//		IndexRequestBuilder request = elasticSearchClient.prepareIndex("consumer_position", "log_position", "1");

//
//		try {
//			XContentBuilder builder = XContentFactory.jsonBuilder();
//			builder.startObject();
//			builder.field("logName", logName);
//			builder.field("logPosition", logPosition);
//			builder.endObject();
//			request.setSource(builder.string());
//			request.get();
//		} catch (IOException e) {
//			throw new BusinessException(e);
//		}

		String data = "{\"logName\":\"" + logName.replaceAll("\"", "'") + "\",\"logPosition\":\"" + logPosition + "\"}";

		this.indexRequest("consumer_position", "log_position", "1", data);
	}

	@Override public void setPosition(long logPosition) {
//		UpdateRequestBuilder request = elasticSearchClient.prepareUpdate("consumer_position", "log_position", "1");
//		request.setDoc("{\"logPosition\": "+ logPosition +"}");
//		request.get();
		LogPosition logFilePosition = this.getCurrentLogPosition();
		String data = "{\"logName\":\"" + logFilePosition.getFilename().replaceAll("\"", "'") + "\",\"logPosition\":\"" + logPosition + "\"}";

		this.indexRequest("consumer_position", "log_position", "1", data);
	}

	@Override public LogPosition getCurrentLogPosition() {
		try {
			//			GetResponse response = elasticSearchClient.prepareGet("consumer_position", "log_position", "1").execute().actionGet();
			//			return new LogPosition(response.getSource().get("logName").toString(), Long.parseLong(response.getSource().get("logPosition").toString()));

			URL url = new URL("https://search-elasticsearch-co-yidbycxnnswqnhvxyyldpnmxpq.sa-east-1.es.amazonaws.com/consumer_position/log_position/1");
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("User-Agent", "Mozilla/5.0");
			BufferedReader in = new BufferedReader(
				new InputStreamReader(connection.getInputStream())
			);

			String inputLine;
			StringBuffer stringResponse = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				stringResponse.append(inputLine);
			}
			in.close();
			JSONObject response = new JSONObject(stringResponse.toString());
			response = response.getJSONObject("_source");

			if (!(connection.getResponseCode() >= 200 && connection.getResponseCode() <= 299)) {
				throw new RuntimeException("Request error at get logPosition: " + connection.getResponseMessage());
			}

			return new LogPosition(response.getString("logName"), Long.parseLong(response.getString("logPosition")));
		} catch (RuntimeException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		} catch (MalformedURLException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		} catch (IOException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		}
	}

	@Override protected void insert(Event event) {
		logger.warning("Insert: ns = " + event.getNamespace() + ", collection: " + event.getCollection() + " valid: " + event.isInsert() + " id: " + event.getData().get("id"));
//		IndexRequestBuilder request = elasticSearchClient.prepareIndex(event.getNamespace(), event.getCollection(), event.getData().get("id").toString());
//		request.setSource(event.getData());
//		request.get();

		this.indexItem(event.getData().get("id").toString(), event.getData());
	}

	@Override protected void update(Event event) {
		logger.warning("Update: ns: " + event.getNamespace() + ", collection: " + event.getCollection() + " valid: " + event.isUpdate() + " id: " + event.getData().get("id"));
//		IndexRequestBuilder request = elasticSearchClient.prepareIndex(event.getNamespace(), event.getCollection(), event.getData().get("id").toString());
//		request.setSource(event.getData());
//		request.get();

		this.indexItem(event.getData().get("id").toString(), event.getData());
	}

	@Override protected void delete(Event event) {
		logger.warning("Delete: ns = " + event.getNamespace() + ", collection: " + event.getCollection() + " valid: " + event.isDelete() + " id: " + event.getConditions().get("id"));
//		DeleteRequestBuilder request = elasticSearchClient.prepareDelete(event.getNamespace(), event.getCollection(), event.getConditions().get("id").toString());
//		request.setId(event.getConditions().get("id").toString());
//		request.get();
		this.deleteIndexedItem(event.getConditions().get("id").toString());
	}

	private void indexItem(String id, Map<String, Object> content) {
//		Config conf = ConfigFactory.load();

		Map<String, String> stringContent = new HashMap<String, String>();

		for (Map.Entry<String, Object> entry : content.entrySet()) {
		    String key = entry.getKey();
		    Object value = entry.getValue();

		    if (value == null) {
		    	value = "";
			}
		    
		    stringContent.put(key.toString(), value.toString());
		}

		Gson gson = new Gson();
		String data = gson.toJson(stringContent);

		this.indexRequest("controle", "licitacoes", id, data);
	}

	private void indexRequest(String index, String type, String id, String content) {
		try{
			URL url = new URL("https://search-elasticsearch-co-yidbycxnnswqnhvxyyldpnmxpq.sa-east-1.es.amazonaws.com/" + index + "/" + type + "/" + id);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("PUT");
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
			connection.setRequestProperty("Accept", "application/json");
			OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);

			content = this.removeMarks(content);

			logger.warning("Indexando licitação: " + id + " com conteudo: " + content);

			osw.write(content);
			osw.flush();
			osw.close();

			if (!(connection.getResponseCode() >= 200 && connection.getResponseCode() <= 299)) {
				throw new RuntimeException("Request error at id " + id + ": " + connection.getResponseMessage());
			}
		} catch (MalformedURLException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		} catch (IOException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		}
	}

	private void deleteIndexedItem(String id) {
//		Config conf = ConfigFactory.load();

		try{
			URL url = new URL("https://search-elasticsearch-co-yidbycxnnswqnhvxyyldpnmxpq.sa-east-1.es.amazonaws.com/controle/licitacoes/" + id);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("DELETE");
			connection.setDoOutput(true);
			OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream());
			osw.flush();
			osw.close();

			if (!(connection.getResponseCode() >= 200 && connection.getResponseCode() <= 299) && connection.getResponseCode() != 404) {
				throw new RuntimeException("Request error at id " + id + ": " + connection.getResponseCode() + connection.getResponseMessage());
			}
		} catch (MalformedURLException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		} catch (IOException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		}
	}

	public String removeMarks(String content){
		String regex = "(/\\*([^*]|[\\r\\n]|(\\*([^/]|[\\r\\n])))*\\*/)|(\n)|(\r)|(\t)";
		return content.replaceAll(regex, " ").replaceAll(" +", " ");

	}

}
