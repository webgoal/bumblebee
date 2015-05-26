package bumblebee.core;

import java.util.Map;

public class Event {

	private String collection;
	private Map<String, Object> data;

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public String getCollection() {
		return collection;
	}
	
	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	public Map<String, Object> getData() {
		return data;
	}

}
