package bumblebee.core.interfaces;

import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;

public interface Consumer {
	void consume(Event event) throws BusinessException;
	void setPositionManager(MySQLPositionManager positionManager) throws BusinessException;
	void setPosition(String logName, long logPosition) throws BusinessException;
	void setPosition(long logPosition) throws BusinessException;
	LogPosition getCurrentLogPosition() throws BusinessException;
	
	void commit();
	void rollback();
}
