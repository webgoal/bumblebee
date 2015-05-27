package bumblebee.core.reader;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import bumblebee.core.Event;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Producer;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogReader implements Producer {
	private static final String host     = "192.168.59.103";
	private static final String user     = "root";
	private static final String pass     = "mypass";
	private static final String database = "db1";
	private static final Integer port    = 3306;
	
	private static final String binlogFilename = "mysql-bin.000004";
	private static final long   binlogPosition = 4L;
	
	private BinaryLogClient client;
	private Consumer consumer;
	
	private Map<Long, String> tableInfo = new HashMap<Long, String>();
	private SchemaManager schemaManager;
	
	public MySQLBinlogReader() {
		client = new BinaryLogClient(host, port, database, user, pass);
		client.setBinlogFilename(binlogFilename);
		client.setBinlogPosition(binlogPosition);
		client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
			@Override public void onConnect(BinaryLogClient client) {
				System.out.println("Conectou!");
			}
			@Override public void onDisconnect(BinaryLogClient client) {
				System.out.println("Desconectou!");
			}
			@Override public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
				System.out.println("Falha na descerialização!");
			}
			@Override public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
				System.out.println("Falha na comunicação!");
			}
		});
		client.registerEventListener(new BinaryLogClient.EventListener() {
			@Override public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
				System.out.println(event);
				if (event.getHeader().getEventType() == EventType.TABLE_MAP)
					MySQLBinlogReader.this.mapTable(event.getData());
				if (event.getHeader().getEventType() == EventType.EXT_WRITE_ROWS)
					MySQLBinlogReader.this.transformInsert(event.getData());
			}
		});
	}
	
	public String getTableById(Long tableId) {
		return tableInfo.get(tableId);
	}
	
	public void setSchemaManager(SchemaManager schemaManager) {
		this.schemaManager = schemaManager;
	}
	
	@Override public void attach(Consumer consumer) {
		this.consumer = consumer;
	}

	public void mapTable(TableMapEventData data) {
		tableInfo.put(data.getTableId(), data.getTable());
	}

	public void transformInsert(WriteRowsEventData data) {
		for (Serializable[] row : data.getRows()) {
			Event event = new Event();
			event.setCollection(tableInfo.get(data.getTableId()));			
			event.setData(dataToMap(tableInfo.get(data.getTableId()), row));
			
			consumer.insert(event);
		}
	}

	private Map<String, Object> dataToMap(String tableName, Serializable[] row) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < row.length; i++)
			map.put(schemaManager.getColumnName(tableName, i), row[i]);
		return map;
	}

	public void start() {
		try {
			client.connect();
		} catch (IOException e) {
			// should try reconnect
			e.printStackTrace();
		}
	}
	
}
