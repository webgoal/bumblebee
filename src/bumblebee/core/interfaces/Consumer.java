package bumblebee.core.interfaces;

import bumblebee.core.Event;

public interface Consumer {
	void insert(Event event);
}
