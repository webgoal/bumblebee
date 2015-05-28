package bumblebee.core.aux;

import bumblebee.core.Event;
import bumblebee.core.interfaces.Consumer;

public class DummyConsumer implements Consumer {
	private Long position;

	public Object lastPosition() {
		return position;
	}

	@Override public void insert(Event event) {
		System.out.println(event);
		position = event.getPosition();
	}

	@Override public void update(Event event) {
		System.out.println(event);
		position = event.getPosition();
	}

	@Override public void delete(Event event) {
		System.out.println(event);
		position = event.getPosition();
	}	
}