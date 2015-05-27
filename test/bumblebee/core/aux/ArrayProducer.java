package bumblebee.core.aux;

import java.util.Map;

import bumblebee.core.Event;
import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Producer;

public class ArrayProducer implements Producer {
	private Consumer consumer;
	
	private Long position;
	private String namespace;
	private String collection;

	public ArrayProducer(Long position, String namespace, String collection) {
		this.position = position;
		this.namespace = namespace;
		this.collection = collection;
	}

	@Override public void attach(Consumer consumer) {
		this.consumer = consumer;
	}
	
	public void doInsert(Map<String, Object> data) {
		Event event = new Event();
		event.setPosition(++position);
		event.setNamespace(namespace);
		event.setCollection(collection);
		event.setData(data);
		consumer.insert(event);
	}
}