package bumblebee.core.applier;

import bumblebee.core.events.Event;
import bumblebee.core.interfaces.Consumer;

public abstract class AbstractConsumer implements Consumer {
	@Override public void consume(Event event) {
		if (event.isInsert()) insert(event);
		if (event.isUpdate()) update(event);
		if (event.isDelete()) delete(event);
	}
	
	abstract protected void insert(Event event);
	
	abstract protected void update(Event event);
	
	abstract protected void delete(Event event);
}
