package bumblebee.samples.transformations;

import bumblebee.core.events.Event;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.transformations.AbstractTransformer;

public class MySQLDelegateTransformer extends AbstractTransformer {
	
	public MySQLDelegateTransformer(Consumer consumer) {
		super(consumer);
	}

	@Override public boolean consume(Event event) {
		return consumer.consume(event);
	}
}
