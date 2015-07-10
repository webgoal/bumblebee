package bumblebee.samples.transformations;

import bumblebee.core.events.Event;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.transformations.AbstractTransformer;

public class MySQLDelegateTransformer extends AbstractTransformer {
	
	public MySQLDelegateTransformer(Consumer consumer) {
		super(consumer);
	}

	@Override public void consume(Event event) {
		consumer.consume(event);
	}
}
