package bumblebee.core.aux;

import java.util.Map;

import bumblebee.core.interfaces.Consumer;

public class FakeApplier implements Consumer {
	private Integer position;

	public Object lastPosition() {
		return position;
	}

	public void insert(Integer position, Map<String, Object> data) {
		this.position = position;
	}	
}