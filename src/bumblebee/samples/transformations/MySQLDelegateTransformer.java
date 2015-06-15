package bumblebee.samples.transformations;

import bumblebee.core.events.Event;
import bumblebee.core.exceptions.BusinessException;
import bumblebee.core.transformations.MySQLAbstractTransformer;

public class MySQLDelegateTransformer extends MySQLAbstractTransformer {
	@Override public void insert(Event event) throws BusinessException {
		consumer.insert(event);
	}
	
	@Override public void update(Event event) throws BusinessException {
		consumer.update(event);
	}
	
	@Override public void delete(Event event) throws BusinessException {
		consumer.delete(event);
	}
}
