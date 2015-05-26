package bumblebee.core;

import java.util.Map;

class ArrayReader {
	private Integer position;
	private FakeApplier applier;

	public ArrayReader(Integer position, String namespace, String collection) {
		this.position = position;
	}

	public void attach(FakeApplier applier) {
		this.applier = applier;
	}

	public void onInsert(Map<String, Object> data) {
		applier.insert(++position, data);
	}
}