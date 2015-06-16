package bumblebee.core.transformations;

import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Transformer;

public abstract class MySQLAbstractTransformer implements Transformer {

	protected Consumer consumer;

	@Override public void setPositionManager(MySQLPositionManager positionManager) { 
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
	
	@Override public void commit() throws BusinessException {
		consumer.commit();
	}
	
	@Override public void rollback() throws BusinessException {
		consumer.rollback();
	}
}
