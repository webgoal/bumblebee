package bumblebee.core.interfaces;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;

public interface Consumer {
	boolean consume(Event event);
	void setPosition(String logName, long logPosition);
	void setPosition(long logPosition);
	LogPosition getCurrentLogPosition();
	
	void commit();
	void rollback();
}
