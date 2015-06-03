package bumblebee.transformer;

import bumblebee.core.Event;
import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Transformer;

public abstract class AbstractTransformer implements Transformer {

	protected Consumer consumer;

	@Override public void insert(Event event) throws BusinessException { }

	@Override public void update(Event event) throws BusinessException { }

	@Override public void delete(Event event) throws BusinessException { }

	@Override public void setPositionManager(MySQLPositionManager positionManager) throws BusinessException { 
		consumer.setPositionManager(positionManager);
	}

	@Override public void setPosition(String logName, long logPosition) throws BusinessException {
		consumer.setPosition(logName, logPosition);
	}

	@Override public void setPosition(long logPosition) throws BusinessException {
		consumer.setPosition(logPosition);
	}

	@Override public LogPosition getCurrentLogPosition() throws BusinessException {
		return consumer.getCurrentLogPosition();
	}

	@Override public void attach(Consumer consumer) {
		this.consumer = consumer; 
	}
	
	@Override public void commit() {
		consumer.commit();
	}
	
	@Override public void rollback() {
		consumer.rollback();
	}
}
