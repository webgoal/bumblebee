package bumblebee.core.reader;

import java.io.IOException;
import java.util.logging.Logger;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.util.MySQLConnectionManager;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;

public class MySQLBinlogConnector implements BinaryLogClient.EventListener {
	private BinaryLogClient client;
	private MySQLBinlogAdapter producer;
	private Logger logger;

	public MySQLBinlogConnector(MySQLBinlogAdapter producer, LogPosition logPosition, long serverId) {
		logger = Logger.getLogger(getClass().getName());
		this.producer = producer;
		client = new BinaryLogClient(MySQLConnectionManager.getProducerHost(), MySQLConnectionManager.getProducerPort(), null, MySQLConnectionManager.getProducerUser(), MySQLConnectionManager.getProducerPass());
		client.setServerId(serverId);
		client.setBinlogFilename(logPosition.getFilename());
		client.setBinlogPosition(logPosition.getPosition());
		client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
			@Override public void onConnect(BinaryLogClient client) {
				logger.info("Conectou!");
			}
			@Override public void onDisconnect(BinaryLogClient client) {
				logger.info("Desconectou!");
			}
			@Override public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
				logger.severe("Falha na desserialização!");
				logger.severe(ex.getMessage());
				throw new BusinessException(ex);
			}
			@Override public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
				logger.severe("Falha na comunicação!");
				logger.severe(ex.getMessage());
				throw new BusinessException(ex);
			}
		});
		client.registerEventListener(this);
	}

	@Override public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
		logger.info(event.toString());
		try {
			if (event.getHeader().getEventType() == EventType.TABLE_MAP)
			producer.mapTable(event.getData());
		} catch (BusinessException ex) {
			ex.printStackTrace();
			logger.severe(ex.getMessage());
			logger.severe("Falha no evento: " + event.getHeader().getEventType())
			disconnect();
			throw new BusinessException(ex);
		}
		try {
			if (event.getHeader().getEventType() == EventType.EXT_WRITE_ROWS)
			producer.transformInsert(event.getData(), (EventHeaderV4) event.getHeader());
		} catch (BusinessException ex) {
			ex.printStackTrace();
			logger.severe(ex.getMessage());
			logger.severe("Falha no evento: " + event.getHeader().getEventType())
			disconnect();
			throw new BusinessException(ex);
		}
		try {
			if (event.getHeader().getEventType() == EventType.EXT_UPDATE_ROWS)
			producer.transformUpdate(event.getData(), (EventHeaderV4) event.getHeader());
		} catch (BusinessException ex) {
			ex.printStackTrace();
			logger.severe(ex.getMessage());
			logger.severe("Falha no evento: " + event.getHeader().getEventType())
			disconnect();
			throw new BusinessException(ex);
		}
		try {
			if (event.getHeader().getEventType() == EventType.EXT_DELETE_ROWS)
			producer.transformDelete(event.getData(), (EventHeaderV4) event.getHeader());
		} catch (BusinessException ex) {
			ex.printStackTrace();
			logger.severe(ex.getMessage());
			logger.severe("Falha no evento: " + event.getHeader().getEventType())
			disconnect();
			throw new BusinessException(ex);
		}
		try {
			if (event.getHeader().getEventType() == EventType.ROTATE)
			producer.changePosition(event.getData());
		} catch (BusinessException ex) {
			ex.printStackTrace();
			logger.severe(ex.getMessage());
			logger.severe("Falha no evento: " + event.getHeader().getEventType())
			disconnect();
			throw new BusinessException(ex);
		}
	}

	private void disconnect() {
		try {
			client.disconnect();
		} catch (IOException e) {
			logger.severe("Falha ao desconectar")
			e.printStackTrace();
		}
	}

	public void connect() {
		try {
			client.setKeepAlive(false);
			client.connect();
		} catch (IOException ex) {
			logger.severe("Falha ao conectar")
			throw new BusinessException(ex);
		}
	}

}
