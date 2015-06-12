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
		dbInfo.put(data.getTableId(), data.getDatabase());
	}

	public void transformInsert(WriteRowsEventData data, EventHeaderV4 eventHeaderV4) throws BusinessException {
		try {
			for (Serializable[] row : data.getRows()) {
				Event event = new InsertEvent();
				event.setNamespace(dbInfo.get(data.getTableId()));
				event.setCollection(tableInfo.get(data.getTableId()));			
				event.setData(dataToMap(tableInfo.get(data.getTableId()), row));
	
				consumer.insert(event);
			}
			consumer.setPosition(eventHeaderV4.getNextPosition());

			consumer.commit();
		}
		catch(BusinessException e) {
			consumer.rollback();
			throw e;
		}
	}

	public void transformUpdate(UpdateRowsEventData data, EventHeaderV4 eventHeaderV4) throws BusinessException {
		try {
			for (Entry<Serializable[], Serializable[]> row : data.getRows()) {
				Event event = new UpdateEvent();
				event.setNamespace(dbInfo.get(data.getTableId()));
				event.setCollection(tableInfo.get(data.getTableId()));
				event.setConditions(dataToMap(tableInfo.get(data.getTableId()), row.getKey()));
				event.setData(dataToMap(tableInfo.get(data.getTableId()), row.getValue()));
	
				consumer.update(event);
			}
			consumer.setPosition(eventHeaderV4.getNextPosition());
			consumer.commit();
		} catch(BusinessException e) {
			consumer.rollback();
			throw e;
		}
	}
	
	public void transformDelete(DeleteRowsEventData data, EventHeaderV4 eventHeaderV4) throws BusinessException {
		try {
			for (Serializable[] row : data.getRows()) {
				Event event = new DeleteEvent();
				event.setNamespace(dbInfo.get(data.getTableId()));
				event.setCollection(tableInfo.get(data.getTableId()));
				event.setConditions(dataToMap(tableInfo.get(data.getTableId()), row));
	
				consumer.delete(event);
			}
			consumer.setPosition(eventHeaderV4.getNextPosition());
			consumer.commit();
		} catch(BusinessException e) {
			consumer.rollback();
			throw e;
		}
	}

	private Map<String, Object> dataToMap(String tableName, Serializable[] row) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < row.length; i++)
			map.put(schemaManager.getColumnName(tableName, i), row[i]);
		return map;
	}

	public void changePosition(RotateEventData data) throws BusinessException {
		consumer.setPosition(data.getBinlogFilename(), data.getBinlogPosition());
	}
}
