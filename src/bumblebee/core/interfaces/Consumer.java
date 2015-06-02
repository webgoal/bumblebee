package bumblebee.core.interfaces;

import util.ConnectionManager;
import bumblebee.core.Event;
import bumblebee.core.applier.MySQLPositionManager;
import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.exceptions.BusinessException;

public interface Consumer {
	void insert(Event event) throws BusinessException;
	void update(Event event) throws BusinessException;
	void delete(Event event) throws BusinessException;
	void setPositionManager(MySQLPositionManager positionManager) throws BusinessException;
	void setPosition(String logName, long logPosition) throws BusinessException;
	void setPosition(long logPosition) throws BusinessException;
	LogPosition getCurrentLogPosition() throws BusinessException;
	void setConnectionManager(ConnectionManager connectionManager);
}
