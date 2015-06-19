package bumblebee.core.events;

import java.util.Collections;
import java.util.Map;

import bumblebee.core.exceptions.BusinessException;

public class Event {

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

	public Map<String, Object> getData() throws BusinessException {
		if (data == null) return Collections.emptyMap();
		return data;
	}
	
	public void setConditions(Map<String, Object> conditions) {
		this.conditions = conditions;		
	}
	
	public Map<String, Object> getConditions() throws BusinessException {
		if (conditions == null) return Collections.emptyMap();
		return conditions;
	}

	@Override public String toString() {
		return "bumblebee.core.Event [namespace=" + namespace
				+ ", collection=" + collection + ", data=" + data + ", conditions="  + conditions+ "]";
	}
	
	public boolean isInsert() throws BusinessException {
		return false;
	}

	public boolean isUpdate() throws BusinessException {
		return false;
	}

	public boolean isDelete() throws BusinessException {
		return false;
	}
}
