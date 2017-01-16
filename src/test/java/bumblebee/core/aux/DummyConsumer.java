package bumblebee.core.aux;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.interfaces.Consumer;

public class DummyConsumer implements Consumer {
	private Event lastEvent;
	private LogPosition lastPosition;
	
	public Event getLastEvent() {
		return lastEvent;
	}

	@Override public boolean consume(Event event) {
		lastEvent = event;
		return true;
	}

	@Override public void setPosition(String logName, long logPosition) {
		this.lastPosition = new LogPosition(logName, logPosition);
	}
	@Override public void setPosition(long logPosition) {
		this.lastPosition = new LogPosition(lastPosition.getFilename(), logPosition);
	}
	@Override public LogPosition getCurrentLogPosition() {
		return lastPosition;
	}

	@Override public void commit() { }

	@Override public void rollback() { }

}