package bumblebee.transformer;

import bumblebee.core.Event;
import bumblebee.core.exceptions.BusinessException;

public class DelegateTransformer extends AbstractTransformer {
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
