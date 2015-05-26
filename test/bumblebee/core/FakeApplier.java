package bumblebee.core;

import java.util.Map;

class FakeApplier {
	private Integer position;

	public Object lastPosition() {
		return position;
	}

	public void insert(Integer position, Map<String, Object> data) {
		this.position = position;
	}	
}