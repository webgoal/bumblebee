package bumblebee.core.applier;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
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
	private String host;

	public ElasticSearchConsumer(String elasticsearchHost) {
		logger = Logger.getLogger(getClass().getName());
		host = elasticsearchHost;
	}

	@Override public void setPosition(String logName, long logPosition) {
		String data = "{\"logName\":\"" + logName.replaceAll("\"", "'") + "\",\"logPosition\":\"" + logPosition + "\"}";

		this.indexRequest("consumer_position", "log_position", "1", data, false);
	}

	@Override public void setPosition(long logPosition) {
		String data = "{\"logPosition\":\"" + logPosition + "\"}";

		this.indexRequest("consumer_position", "log_position", "1", data, true);
	}

	@Override public LogPosition getCurrentLogPosition() {
		try {
			URL url = new URL(host + "/consumer_position/log_position/1");
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
		this.indexItem(event.getNamespace(), event.getCollection(), event.getData().get("id").toString(), event.getData(), false);
	}

	@Override protected void update(Event event) {
		logger.warning("Update: ns: " + event.getNamespace() + ", collection: " + event.getCollection() + " valid: " + event.isUpdate() + " id: " + event.getData().get("id"));
		this.indexItem(event.getNamespace(), event.getCollection(), event.getData().get("id").toString(), event.getData(), true);
	}

	@Override protected void delete(Event event) {
		logger.warning("Delete: ns = " + event.getNamespace() + ", collection: " + event.getCollection() + " valid: " + event.isDelete() + " id: " + event.getConditions().get("id"));
		this.deleteIndexedItem(event.getNamespace(), event.getCollection(), event.getConditions().get("id").toString());
	}

	private void indexItem(String index, String type, String id, Map<String, Object> content, Boolean isUpdate ) {
		Map<String, String> stringContent = new HashMap<String, String>();

		for (Map.Entry<String, Object> entry : content.entrySet()) {
		    String key = entry.getKey();
		    Object value = entry.getValue();

		    if (value != null && !value.equals("")) {
		    	if (value.getClass() == java.util.Date.class) {
			    	value = new SimpleDateFormat("yyyy-MM-dd").format(value);
			    }

			    stringContent.put(key.toString(), value.toString());
		    }
		}

		Gson gson = new Gson();
		String data = gson.toJson(stringContent);

		this.indexRequest(index, type, id, data, isUpdate);
	}

	private void indexRequest(String index, String type, String id, String content, Boolean isUpdate) {
		try{
			String method;
			String urlString = host + "/" + index + "/" + type + "/" + id;
			String contentForRequest;
			
			if (isUpdate) {
				method = "POST";
				urlString += "/_update";
				contentForRequest = "{ \"doc\":" + content + "}";
			} else {
				method = "PUT";
				contentForRequest = content;
			}
			
			URL url = new URL(urlString);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod(method);
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
			connection.setRequestProperty("Accept", "application/json");
			OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);

			contentForRequest = this.removeMarks(contentForRequest);

			System.out.println("Indexando " + type + ": " + id + " com conteudo: " + contentForRequest);

			osw.write(contentForRequest);
			osw.flush();
			osw.close();

			if (!(connection.getResponseCode() >= 200 && connection.getResponseCode() <= 299)) {
				throw new RuntimeException("Request error at id " + id + ": " + connection.getResponseMessage());
			}
			
			if (isUpdate && connection.getResponseCode() >= 404) {
				this.indexRequest(index, type, id, content, false);
			}
		} catch (MalformedURLException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		} catch (IOException e) {
			logger.severe(e.toString());
			throw new BusinessException(e);
		}
	}

	private void deleteIndexedItem(String index, String type, String id) {
		try{
			URL url = new URL(host + "/" + index + "/" + type + "/" + id);
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
