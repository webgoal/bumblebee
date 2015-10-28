package bumblebee.core.reader;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Logger;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.io.BufferedSocketInputStream;
import com.github.shyiko.mysql.binlog.network.SocketFactory;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.util.MySQLConnectionManager;

public class MySQLBinlogConnector implements BinaryLogClient.EventListener {
	private BinaryLogClient client;
	private MySQLBinlogAdapter producer;
	private Logger logger;
	private final int timeoutSeconds = 30000;

	public MySQLBinlogConnector(MySQLBinlogAdapter producer, LogPosition logPosition) {
		logger = Logger.getLogger(getClass().getName());
		this.producer = producer;
		client = new BinaryLogClient(MySQLConnectionManager.getProducerHost(), MySQLConnectionManager.getProducerPort(), null, MySQLConnectionManager.getProducerUser(), MySQLConnectionManager.getProducerPass());
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
		try {
			logger.info(event.toString());
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
			logger.severe(ex.getMessage());
			disconnect();
			throw new BusinessException(ex);
		}
	}

	private void disconnect() {
		try {
			client.disconnect();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void connect() {
		try {
			client.setKeepAlive(false);
			client.setSocketFactory(new SocketFactory() { @Override public Socket createSocket() throws SocketException {
				Socket mySocket = new Socket() {
	                private InputStream inputStream;
	                @Override public synchronized InputStream getInputStream() throws IOException {
	                    return inputStream != null ? inputStream : (inputStream = new BufferedSocketInputStream(super.getInputStream()));
	                }
	            };
	            mySocket.setSoTimeout(timeoutSeconds);
				return mySocket;
			}});
			client.connect();
		} catch (IOException ex) {
			throw new BusinessException(ex);
		}
	}
	
}
