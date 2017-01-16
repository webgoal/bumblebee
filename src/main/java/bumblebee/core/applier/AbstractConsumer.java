package bumblebee.core.applier;

import java.util.logging.Logger;

import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;

public abstract class AbstractConsumer implements Consumer {
	@Override public boolean consume(Event event) {
		try {
			if (event.isInsert()) insert(event);
			else if (event.isUpdate()) update(event);
			else if (event.isDelete()) delete(event);
			return event.isInsert() || event.isUpdate() || event.isDelete();
		} catch(RuntimeException e) {
			Logger.getLogger(getClass().getName()).severe(e.toString());
			throw new BusinessException(e);
		}
	}

	abstract protected void insert(Event event);

	abstract protected void update(Event event);

	abstract protected void delete(Event event);
}
