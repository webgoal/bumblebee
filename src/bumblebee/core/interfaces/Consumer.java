package bumblebee.core.interfaces;

import bumblebee.core.Event;
import bumblebee.core.exceptions.BusinessException;

public interface Consumer {
	void insert(Event event) throws BusinessException;
	void update(Event event) throws BusinessException;
	void delete(Event event) throws BusinessException;
}
