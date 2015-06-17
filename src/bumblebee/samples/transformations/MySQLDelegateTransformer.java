package bumblebee.samples.transformations;

import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.transformations.MySQLAbstractTransformer;

public class MySQLDelegateTransformer extends MySQLAbstractTransformer {
	
	public MySQLDelegateTransformer(Consumer consumer) {
		super(consumer);
	}

	@Override public void consume(Event event) throws BusinessException {
		consumer.consume(event);
	}
}
