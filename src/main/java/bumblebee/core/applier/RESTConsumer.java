package bumblebee.core.applier;

import bumblebee.core.applier.MySQLPositionManager.LogPosition;
import bumblebee.core.applier.rest.JSONEventTranslator;
import bumblebee.core.events.Event;
import bumblebee.core.interfaces.Consumer;

public class RESTConsumer implements Consumer {
	
//	private JSONEventTranslator translator;

	public RESTConsumer(JSONEventTranslator translator) {
//		this.translator = translator;
	}

	@Override
	public void consume(Event event) {
		if (event.isInsert()) insert(event);
	}

	private void insert(Event event) {
		
	}

	@Override
	public void setPosition(String logName, long logPosition) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPosition(long logPosition) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public LogPosition getCurrentLogPosition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub
		
	}

}
