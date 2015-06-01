package bumblebee.core.aux;

import bumblebee.core.Event;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;

public class DummyConsumer implements Consumer {
	private Event lastEvent;
	private LogPosition lastPosition;
	
	public Event getLastEvent() {
		return lastEvent;
	}

	@Override public void insert(Event event) {
		lastEvent = event;
	}
	@Override public void update(Event event) {
		lastEvent = event;
	}
	@Override public void delete(Event event) {
		lastEvent = event;
	}
	@Override public void setPosition(String logName, long logPosition) throws BusinessException {
		this.lastPosition = new LogPosition(logName, logPosition);
	}

	@Override public LogPosition getCurrentLogPosition() {
		return lastPosition;
	}
}