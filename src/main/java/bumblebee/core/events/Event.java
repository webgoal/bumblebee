package bumblebee.core.events;

import java.util.Collections;
import java.util.Map;

public abstract class Event {

	private String namespace;
	private String collection;
	private Map<String, Object> data;
	private Map<String, Object> conditions;
	
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
		if (data == null) return Collections.emptyMap();
		return data;
	}
	
	public void setConditions(Map<String, Object> conditions) {
		this.conditions = conditions;		
	}
	
	public Map<String, Object> getConditions() {
		if (conditions == null) return Collections.emptyMap();
		return conditions;
	}

	@Override public String toString() {
		return "bumblebee.core.Event [namespace=" + getNamespace()
				+ ", collection=" + getCollection() + ", data=" + getData() + ", conditions="  + getConditions() + "]";
	}
	
	public boolean isInsert() {
		return false;
	}

	public boolean isUpdate() {
		return false;
	}

	public boolean isDelete() {
		return false;
	}

}
