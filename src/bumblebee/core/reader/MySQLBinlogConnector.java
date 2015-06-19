package bumblebee.core.reader;

import java.io.IOException;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.util.MySQLConnectionManager;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;

public class MySQLBinlogConnector implements BinaryLogClient.EventListener {
	private BinaryLogClient client;
	private MySQLBinlogAdapter producer;

	public MySQLBinlogConnector(MySQLBinlogAdapter producer, LogPosition logPosition) {
		this.producer = producer;
		client = new BinaryLogClient(MySQLConnectionManager.getProducerHost(), MySQLConnectionManager.getProducerPort(), null, MySQLConnectionManager.getProducerUser(), MySQLConnectionManager.getProducerPass());
		client.setBinlogFilename(logPosition.getFilename());
		client.setBinlogPosition(logPosition.getPosition());
		client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
			@Override public void onConnect(BinaryLogClient client) {
				System.out.println("Conectou!");
			}
			@Override public void onDisconnect(BinaryLogClient client) {
				System.out.println("Desconectou!");
			}
			@Override public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
				System.out.println("Falha na desserialização!");
			}
			@Override public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
				System.out.println("Falha na comunicação!");
			}
		});
		client.registerEventListener(this);
	}
	
	@Override public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
		try {
			System.out.println(event);
			if (event.getHeader().getEventType() == EventType.TABLE_MAP)
				producer.mapTable(event.getData());
			if (event.getHeader().getEventType() == EventType.EXT_WRITE_ROWS)
				producer.transformInsert(event.getData(), (EventHeaderV4) event.getHeader());
			if (event.getHeader().getEventType() == EventType.EXT_UPDATE_ROWS)
				producer.transformUpdate(event.getData(), (EventHeaderV4) event.getHeader());
			if (event.getHeader().getEventType() == EventType.EXT_DELETE_ROWS)
				producer.transformDelete(event.getData(), (EventHeaderV4) event.getHeader());
			if (event.getHeader().getEventType() == EventType.ROTATE)
				producer.changePosition(event.getData());
		} catch (BusinessException ex) {
			ex.printStackTrace();
			disconnect();
		}
	}

	public void connect() {
		try {
			client.connect();
		} catch (IOException ex) {
			throw new BusinessException(ex);
		}
	}
	
	private void disconnect() {
		try {
			client.disconnect();
		} catch (IOException ex) {
			throw new BusinessException(ex);
		}
	}
}
