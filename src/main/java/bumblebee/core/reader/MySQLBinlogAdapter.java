package bumblebee.core.reader;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import bumblebee.core.events.DeleteEvent;
import bumblebee.core.events.Event;
import bumblebee.core.events.InsertEvent;
import bumblebee.core.events.UpdateEvent;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Producer;
import bumblebee.core.interfaces.SchemaManager;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MySQLBinlogAdapter implements Producer {
	private Consumer consumer;

	private Map<Long, String> tableInfo = new HashMap<Long, String>();
	private Map<Long, String> dbInfo = new HashMap<Long, String>();
	private SchemaManager schemaManager;

	private long sinceLastCommit = 0;

	public MySQLBinlogAdapter(Consumer consumer, SchemaManager schemaManager) {
		this.consumer = consumer;
		this.schemaManager = schemaManager;
	}

	public String getTableById(Long tableId) {
		return tableInfo.get(tableId);
	}

	public void mapTable(TableMapEventData data) {
		tableInfo.put(data.getTableId(), data.getTable());
		dbInfo.put(data.getTableId(), data.getDatabase());
	}

	public void transformInsert(WriteRowsEventData data, EventHeaderV4 eventHeaderV4) {
		try {
			boolean force = false;
			for (Serializable[] row : data.getRows()) {
				Event event = new InsertEvent();
				event.setNamespace(dbInfo.get(data.getTableId()));
				event.setCollection(tableInfo.get(data.getTableId()));
				event.setData(dataToMap(dbInfo.get(data.getTableId()), tableInfo.get(data.getTableId()), row));

				force = consumer.consume(event);
			}
			updateAndCommitIfNeeded(eventHeaderV4.getNextPosition(), force);
//			consumer.setPosition(eventHeaderV4.getNextPosition());
//			consumer.commit();
		} catch(BusinessException e) {
			consumer.rollback();
			throw e;
		}
	}

	public void transformUpdate(UpdateRowsEventData data, EventHeaderV4 eventHeaderV4) {
		try {
			boolean force = false;
			for (Entry<Serializable[], Serializable[]> row : data.getRows()) {
				Event event = new UpdateEvent();
				event.setNamespace(dbInfo.get(data.getTableId()));
				event.setCollection(tableInfo.get(data.getTableId()));
				event.setConditions(dataToMap(dbInfo.get(data.getTableId()), tableInfo.get(data.getTableId()), row.getKey()));
				event.setData(dataToMap(dbInfo.get(data.getTableId()), tableInfo.get(data.getTableId()), row.getValue()));

				force = consumer.consume(event);
			}
			updateAndCommitIfNeeded(eventHeaderV4.getNextPosition(), force);
//			consumer.setPosition(eventHeaderV4.getNextPosition());
//			consumer.commit();
		} catch(BusinessException e) {
			consumer.rollback();
			throw e;
		}
	}

	public void transformDelete(DeleteRowsEventData data, EventHeaderV4 eventHeaderV4) {
		try {
			boolean force = false;
			for (Serializable[] row : data.getRows()) {
				Event event = new DeleteEvent();
				event.setNamespace(dbInfo.get(data.getTableId()));
				event.setCollection(tableInfo.get(data.getTableId()));
				event.setConditions(dataToMap(dbInfo.get(data.getTableId()), tableInfo.get(data.getTableId()), row));
				force = consumer.consume(event);
			}
			updateAndCommitIfNeeded(eventHeaderV4.getNextPosition(), force);
//			consumer.setPosition(eventHeaderV4.getNextPosition());
//			consumer.commit();
		} catch(BusinessException e) {
			consumer.rollback();
			throw e;
		}
	}

	private void updateAndCommitIfNeeded(long nextPosition, boolean force) {
		if (sinceLastCommit++ > 1000 || force) {
			consumer.setPosition(nextPosition);
			consumer.commit();
			sinceLastCommit = 0;
		}

	}

	private Map<String, Object> dataToMap(String dbName, String tableName, Serializable[] row) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < row.length; i++)
			map.put(schemaManager.getColumnName(dbName, tableName, i), row[i]);
		return map;
	}

	public void changePosition(RotateEventData data) {
		consumer.setPosition(data.getBinlogFilename(), data.getBinlogPosition());
	}
}
