package bumblebee.core.interfaces;

import util.ConnectionManager;

public interface Producer {
	void attach(Consumer consumer);
	void setConnectionManager(ConnectionManager connectionManager);
}
