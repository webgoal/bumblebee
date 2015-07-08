package bumblebee.core.transformations;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Transformer;

public abstract class MySQLAbstractTransformer implements Transformer {

	protected Consumer consumer;
	
	public MySQLAbstractTransformer(Consumer consumer) {
		this.consumer = consumer;
	}

	@Override public void setPosition(String logName, long logPosition) {
		consumer.setPosition(logName, logPosition);
	}

	@Override public void setPosition(long logPosition) {
		consumer.setPosition(logPosition);
	}

	@Override public LogPosition getCurrentLogPosition() {
		return consumer.getCurrentLogPosition();
	}

	@Override public void commit() {
		consumer.commit();
	}
	
	@Override public void rollback() {
		consumer.rollback();
	}
}
