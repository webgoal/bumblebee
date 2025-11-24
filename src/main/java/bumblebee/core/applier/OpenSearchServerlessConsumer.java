package bumblebee.core.applier;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.io.*;

import org.json.JSONObject;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.HttpMethodName;
import com.google.gson.Gson;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;

public class OpenSearchServerlessConsumer extends RESTConsumer {
	private Logger logger;
	private String host;
	private String region;
	private AWSCredentialsProvider credentialsProvider;
	private AWS4Signer signer;

	public OpenSearchServerlessConsumer(String opensearchHost, String region) {
		logger = Logger.getLogger(getClass().getName());
		this.host = opensearchHost;
		this.region = region != null ? region : "us-east-1";
		this.credentialsProvider = new DefaultAWSCredentialsProviderChain();
		
		this.signer = new AWS4Signer();
		this.signer.setServiceName("aoss"); // Amazon OpenSearch Serverless
		this.signer.setRegionName(this.region);
	}

	@Override public void setPosition(String logName, long logPosition) {
		String data = "{\"logName\":\"" + logName.replaceAll("\"", "'") + "\",\"logPosition\":\"" + logPosition + "\"}";
		this.indexRequest("log_position", "1", data, true);
	}

	@Override public void setPosition(long logPosition) {
		String data = "{\"logPosition\":\"" + logPosition + "\"}";
		this.indexRequest("log_position", "1", data, true);
	}

	@Override public LogPosition getCurrentLogPosition() {
		logger.info("Entrou no getCurrentLogPosition");
		try {
			URL url = new URL(host + "/log_position/_doc/1");
			HttpURLConnection connection = createSignedConnection(url, "GET", null);
			
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
		logger.info("Insert: ns = " + event.getNamespace() + ", collection: " + event.getCollection() + " valid: " + event.isInsert() + " id: " + event.getData().get("id"));
		this.indexItem(event.getCollection(), event.getData().get("id").toString(), event.getData(), false);
	}

	@Override protected void update(Event event) {
		logger.info("Update: ns: " + event.getNamespace() + ", collection: " + event.getCollection() + " valid: " + event.isUpdate() + " id: " + event.getData().get("id"));
		this.indexItem(event.getCollection(), event.getData().get("id").toString(), event.getData(), true);
	}

	@Override protected void delete(Event event) {
		logger.warning("Delete: ns = " + event.getNamespace() + ", collection: " + event.getCollection() + " valid: " + event.isDelete() + " id: " + event.getConditions().get("id"));
		this.deleteIndexedItem(event.getCollection(), event.getConditions().get("id").toString());
	}

	private void indexItem(String index, String id, Map<String, Object> content, Boolean isUpdate ) {
		Map<String, String> stringContent = new HashMap<String, String>();

		for (Map.Entry<String, Object> entry : content.entrySet()) {
		    String key = entry.getKey();
		    Object value = entry.getValue();

		    if (value != null && !value.equals("")) {
					
		    	if (value.getClass() == java.util.Date.class) {
			    	value = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(value);
			    }

					if (key.toString().equals("edital_tem")) {
						if(value.toString().equals("0")) value = false;
						if(value.toString().equals("1")) value = true;
					}

			    stringContent.put(key.toString(), value.toString());
		    }
		}

		Gson gson = new Gson();
		String data = gson.toJson(stringContent);

		this.indexRequest(index, id, data, isUpdate);
	}

	private void indexRequest(String index, String id, String content, Boolean isUpdate) {
		try{
			String method;
			String urlString;
			String contentForRequest;

			if (isUpdate) {
				method = "POST";
				urlString = host + "/" + index + "/_update/" + id;
				contentForRequest = "{ \"doc\":" + content + "}";	
			} else {
				method = "PUT";
				urlString = host + "/" + index + "/_create/" + id;
				contentForRequest = content;
			}
		
			URL url = new URL(urlString);
			contentForRequest = this.removeMarks(contentForRequest);

			HttpURLConnection connection = createSignedConnection(url, method, contentForRequest);

			System.out.println("Indexando " + index + ": " + id + " com conteudo: " + contentForRequest);	

			if (contentForRequest != null && !contentForRequest.isEmpty()) {
				OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8);
				osw.write(contentForRequest);
				osw.flush();
				osw.close();
			}

			if (isUpdate && connection.getResponseCode() >= 404) {
				this.indexRequest(index, id, content, false);
				return;
			}

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

	private void deleteIndexedItem(String index, String id) {
		try{
			URL url = new URL(host + "/" + index + "/_doc/" + id);
			HttpURLConnection connection = createSignedConnection(url, "DELETE", null);

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

	private HttpURLConnection createSignedConnection(URL url, String method, String content) throws IOException {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod(method);
		connection.setDoOutput(true);
		connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
		connection.setRequestProperty("Accept", "application/json");

		// Criar request para assinatura AWS
		DefaultRequest<?> request = new DefaultRequest<>("aoss");
		request.setHttpMethod(HttpMethodName.valueOf(method));
		try {
			request.setEndpoint(url.toURI());
		} catch (java.net.URISyntaxException e) {
			throw new IOException("Invalid URL: " + url.toString(), e);
		}
		request.setResourcePath(url.getPath() + (url.getQuery() != null ? "?" + url.getQuery() : ""));

		// Adicionar headers
		request.addHeader("Content-Type", "application/json;charset=UTF-8");
		request.addHeader("Accept", "application/json");

		// Adicionar conteúdo se houver
		if (content != null && !content.isEmpty()) {
			byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
			request.setContent(new ByteArrayInputStream(contentBytes));
			request.addHeader("Content-Length", String.valueOf(contentBytes.length));
		}

		// Assinar a requisição
		try {
			AWSCredentials credentials = credentialsProvider.getCredentials();
			signer.sign(request, credentials);
		} catch (Exception e) {
			logger.severe("Erro ao assinar requisição: " + e.toString());
			throw new BusinessException(e);
		}

		// Aplicar headers assinados na conexão
		for (Map.Entry<String, String> entry : request.getHeaders().entrySet()) {
			connection.setRequestProperty(entry.getKey(), entry.getValue());
		}

		return connection;
	}

	public String removeMarks(String content){
		String regex = "(\\n)|(\\r)|(\\t)";
		return content.replaceAll(regex, " ").replaceAll(" +", " ");
	}
}

