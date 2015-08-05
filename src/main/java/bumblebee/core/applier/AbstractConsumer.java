package bumblebee.core.applier;

import com.sun.istack.internal.logging.Logger;

import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;

public abstract class AbstractConsumer implements Consumer {
	@Override public void consume(Event event) {
		try {
			if (event.isInsert()) insert(event);
			if (event.isUpdate()) update(event);
			if (event.isDelete()) delete(event);
		} catch(RuntimeException e) {
			Logger.getLogger(getClass()).severe(e.toString());
			throw new BusinessException(e);
		}
	}
	
	abstract protected void insert(Event event);
	
	abstract protected void update(Event event);
	
	abstract protected void delete(Event event);
}
