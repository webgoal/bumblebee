package bumblebee.core.reader;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;

public class MySQLBinlogConnector implements BinaryLogClient.EventListener {
	private static final String host     = "192.168.59.103";
	private static final String user     = "root";
	private static final String pass     = "mypass";
	private static final String database = "db1";
	private static final Integer port    = 3306;
	
	private static final String binlogFilename = "mysql-bin.000004";
	private static final long   binlogPosition = 4L;
	
	private BinaryLogClient client;
	private MySQLBinlogAdapter producer;

	public MySQLBinlogConnector() {
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
		client.registerEventListener(this);
	}
	
	public void setAdapter(MySQLBinlogAdapter producer) {
		this.producer = producer;
	}
	
	public void start() {
		try {
			client.connect();
		} catch (IOException e) {
			// should try reconnect
			e.printStackTrace();
		}
	}
	
	@Override public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
		System.out.println(event);
		if (event.getHeader().getEventType() == EventType.TABLE_MAP)
			MySQLBinlogConnector.this.producer.mapTable(event.getData());
		if (event.getHeader().getEventType() == EventType.EXT_WRITE_ROWS)
			MySQLBinlogConnector.this.producer.transformInsert(event.getData());
	}

}
