package bumblebee.core;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;

public class Main {
	private String host     = "192.168.59.103";
	private String user     = "root";
	private String pass     = "mypass";
	private String database = "db1";
	private Integer port    = 3306;
	
	private String binlogFilename = "mysql-bin.000004";
	private long binlogPosition = 1L;
	
	public static void main(String[] args) throws IOException {
		new Main();
	}
	
	public Main() throws IOException {
		BinaryLogClient client = new BinaryLogClient(host, port, database, user, pass);
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
			@Override public void onEvent(Event event) {
				System.out.println(event);
			}
		});
		client.connect();
	}
}
