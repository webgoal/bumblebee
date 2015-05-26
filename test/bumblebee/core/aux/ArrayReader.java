package bumblebee.core.aux;

import java.util.Map;

import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Producer;

public class ArrayReader implements Producer {
	private Integer position;
	private Consumer consumer;

	public ArrayReader(Integer position, String namespace, String collection) {
		this.position = position;
	}

	@Override public void attach(Consumer consumer) {
		this.consumer = consumer;
	}
	
	@Override public void onInsert(Map<String, Object> data) {
		consumer.insert(++position, data);
	}
}