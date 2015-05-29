package bumblebee.core;

import java.util.Map;

public class Event {

	private Long position;
	private String namespace;
	private String collection;
	private Map<String, Object> data;
	private Map<String, Object> conditions;

	public void setPosition(Long position) {
		this.position = position;
	}
	
	public Long getPosition() {
		return position;
	}
	
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	
	public String getNamespace() {
		return namespace;
	}
	
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
	
	public void setCondition(Map<String, Object> conditions) {
		this.conditions = conditions;		
	}
	
	public Map<String, Object> getConditions() {
		return conditions;
	}

	@Override public String toString() {
		return "bumblebee.core.Event [position=" + position + ", namespace=" + namespace
				+ ", collection=" + collection + ", data=" + data + ", conditions="  + conditions+ "]";
	}
}
