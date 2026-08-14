package bumblebee.core.applier;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.json.JSONObject;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Result;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.UpdateRequest;
import org.opensearch.client.opensearch.core.UpdateResponse;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;

import com.google.gson.Gson;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;

public class OpenSearchServerlessConsumer extends RESTConsumer {
	private static final Set<String> BOOLEAN_FIELDS = new HashSet<>(Arrays.asList(
		"edital_tem", "garantia_proposta", "garantia_contrato"
	));

	private Logger logger;
	private String host;
	private String region;
	private OpenSearchClient client;

	public OpenSearchServerlessConsumer(String opensearchHost, String region) {
		logger = Logger.getLogger(getClass().getName());
		this.host = opensearchHost;
		this.region = region != null ? region : "us-east-1";
		
		try {
			SdkHttpClient httpClient = ApacheHttpClient.builder().build();
			Region awsRegion = Region.of(this.region);
			
			AwsSdk2TransportOptions transportOptions = AwsSdk2TransportOptions.builder()
				.build();
			
			String endpoint = opensearchHost;
			if (endpoint.startsWith("https://")) {
				endpoint = endpoint.substring(8);
			} else if (endpoint.startsWith("http://")) {
				endpoint = endpoint.substring(7);
			}

			this.client = new OpenSearchClient(
				new AwsSdk2Transport(
					httpClient,
					endpoint,
					"aoss",
					awsRegion,
					transportOptions
				)
			);
			
			logger.info("OpenSearch Serverless client initialized successfully");
		} catch (Exception e) {
			logger.severe("Erro ao inicializar cliente OpenSearch Serverless: " + e.toString());
			e.printStackTrace();
			throw new BusinessException(e);
		}
	}

	@Override
	public void setPosition(String logName, long logPosition) {
		Map<String, Object> data = new HashMap<>();
		data.put("logName", logName);
		data.put("logPosition", logPosition);
		this.indexRequest("log_position", "1", data, true);
	}

	@Override
	public void setPosition(long logPosition) {
		Map<String, Object> data = new HashMap<>();
		data.put("logPosition", logPosition);
		this.indexRequest("log_position", "1", data, true);
	}

	@Override
	public LogPosition getCurrentLogPosition() {
		try {
			GetRequest getRequest = GetRequest.of(g -> g
				.index("log_position")
				.id("1")
			);
			
			GetResponse<JsonData> response = client.get(getRequest, JsonData.class);
			
			if (!response.found()) {
				logger.warning("Log position not found, returning default");
				return new LogPosition("", 0L);
			}
			
			JsonData source = response.source();
			JSONObject jsonSource = new JSONObject(source.toString());
			
			String logName = jsonSource.optString("logName", "");
			long logPosition = jsonSource.optLong("logPosition", 0L);
			
			return new LogPosition(logName, logPosition);
		} catch (OpenSearchException e) {
			logger.severe("OpenSearch error getting log position: " + e.toString());
			if (e.status() == 404) {
				logger.warning("Log position not found, returning default");
				return new LogPosition("", 0L);
			}
			throw new BusinessException(e);
		} catch (IOException e) {
			logger.severe("IOException getting log position: " + e.toString());
			e.printStackTrace();
			throw new BusinessException(e);
		}
	}

	@Override
	protected void insert(Event event) {
		logger.info("Insert: ns = " + event.getNamespace() + ", collection: " + event.getCollection() 
			+ " valid: " + event.isInsert() + " id: " + event.getData().get("id"));
		this.indexItem(event.getCollection(), event.getData().get("id").toString(), event.getData(), false);
	}

	@Override
	protected void update(Event event) {
		logger.info("Update: ns: " + event.getNamespace() + ", collection: " + event.getCollection() 
			+ " valid: " + event.isUpdate() + " id: " + event.getData().get("id"));
		this.indexItem(event.getCollection(), event.getData().get("id").toString(), event.getData(), true);
	}

	@Override
	protected void delete(Event event) {
		logger.warning("Delete: ns = " + event.getNamespace() + ", collection: " + event.getCollection() 
			+ " valid: " + event.isDelete() + " id: " + event.getConditions().get("id"));
		this.deleteIndexedItem(event.getCollection(), event.getConditions().get("id").toString());
	}

	private void indexItem(String index, String id, Map<String, Object> content, Boolean isUpdate) {
		Map<String, Object> processedContent = new HashMap<>();

		for (Map.Entry<String, Object> entry : content.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();

			if (value != null && !value.equals("")) {
				if (value.getClass() == java.util.Date.class) {
					value = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(value);
				}

				if (BOOLEAN_FIELDS.contains(key.toString())) {
					value = toOpenSearchBoolean(value);
				}

				// Preservar tipos: números como números, booleans como booleans, etc
				processedContent.put(key.toString(), value);
			}
		}

		this.indexRequest(index, id, processedContent, isUpdate);
	}

	private void indexRequest(String index, String id, Map<String, Object> content, Boolean isUpdate) {
		try {
			UpdateRequest<JsonData, JsonData> updateRequest = UpdateRequest.of(u -> u
				.index(index)
				.id(id)
				.doc(JsonData.of(content))
				.docAsUpsert(true)
			);

			System.out.println("Indexando " + index + ": " + id + " com conteudo: " + content);

			UpdateResponse<JsonData> response = client.update(updateRequest, JsonData.class);
			
			if (response.result() == Result.Created || response.result() == Result.Updated) {
				logger.info("Documento indexado com sucesso: " + index + "/" + id + " - Result: " + response.result());
			} else {
				logger.warning("Resultado inesperado ao indexar: " + index + "/" + id + " - Result: " + response.result());
			}
		} catch (OpenSearchException e) {
			String errorDetails = String.format(
				"=== ERRO OpenSearch ao indexar ===\n" +
				"Index: %s\n" +
				"ID: %s\n" +
				"Status: %d\n" +
				"Error: %s\n" +
				"===================",
				index, id, e.status(), e.getMessage()
			);
			logger.severe(errorDetails);
			System.err.println(errorDetails);
			throw new BusinessException(e);
		} catch (IOException e) {
			String errorDetails = String.format(
				"=== ERRO IOException ao indexar ===\n" +
				"Index: %s\n" +
				"ID: %s\n" +
				"Error: %s\n" +
				"===================",
				index, id, e.getMessage()
			);
			logger.severe(errorDetails);
			System.err.println(errorDetails);
			e.printStackTrace();
			throw new BusinessException(e);
		} catch (Exception e) {
			String errorDetails = String.format(
				"=== ERRO ao indexar ===\n" +
				"Index: %s\n" +
				"ID: %s\n" +
				"Error: %s\n" +
				"===================",
				index, id, e.getMessage()
			);
			logger.severe(errorDetails);
			System.err.println(errorDetails);
			e.printStackTrace();
			throw new BusinessException(e);
		}
	}

	private void deleteIndexedItem(String index, String id) {
		try {
			DeleteRequest deleteRequest = DeleteRequest.of(d -> d
				.index(index)
				.id(id)
			);

			DeleteResponse response = client.delete(deleteRequest);

			if (response.result() == Result.Deleted) {
				logger.info("Documento deletado com sucesso: " + index + "/" + id);
			} else if (response.result() == Result.NotFound) {
				logger.warning("Documento não encontrado para deletar: " + index + "/" + id);
			} else {
				logger.warning("Resultado inesperado ao deletar: " + index + "/" + id + " - Result: " + response.result());
			}
		} catch (OpenSearchException e) {
			if (e.status() == 404) {
				logger.warning("Documento não encontrado para deletar: " + index + "/" + id);
				return; // Não é erro se não existir
			}
			logger.severe("Erro OpenSearch ao deletar: " + e.toString());
			throw new BusinessException(e);
		} catch (IOException e) {
			logger.severe("Erro IOException ao deletar: " + e.toString());
			e.printStackTrace();
			throw new BusinessException(e);
		}
	}

	public String removeMarks(String content) {
		String regex = "(\\n)|(\\r)|(\\t)";
		return content.replaceAll(regex, " ").replaceAll(" +", " ");
	}

	private Object toOpenSearchBoolean(Object value) {
		if (value instanceof Boolean) {
			return value;
		}
		if (value instanceof Number) {
			return ((Number) value).intValue() != 0;
		}
		if (value instanceof byte[]) {
			byte[] bytes = (byte[]) value;
			return bytes.length > 0 && bytes[0] != 0;
		}
		String asString = value.toString();
		if ("0".equals(asString) || "false".equalsIgnoreCase(asString)) {
			return false;
		}
		if ("1".equals(asString) || "true".equalsIgnoreCase(asString)) {
			return true;
		}
		return value;
	}
}
