package bumblebee.core.interfaces;

import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;

public interface Consumer {
	void consume(Event event) throws BusinessException;
	void setPositionManager(MySQLPositionManager positionManager);
	void setPosition(String logName, long logPosition) throws BusinessException;
	void setPosition(long logPosition) throws BusinessException;
	LogPosition getCurrentLogPosition() throws BusinessException;
	
	void commit() throws BusinessException;
	void rollback() throws BusinessException;
}
