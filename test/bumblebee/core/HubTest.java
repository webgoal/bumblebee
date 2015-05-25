package bumblebee.core;

import static org.junit.Assert.*;

import java.awt.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class HubTest {

	@Test public void shouldReplicateInsert() {
		Integer position = 0;
		String namespace = "ns";
		String collection = "coll";
		Map<String, Object> data = new HashMap<String, Object>();
		
		ArrayReader arrayReader = new ArrayReader(position, namespace, collection);
		
		FakeApplier applier = new FakeApplier();
		
		new Hub(arrayReader, applier);
		
		arrayReader.insert("first data");
		arrayReader.insert("second data");
		
		Integer expectedPosition = 2;
		assertEquals(expectedPosition, applier.lastPosition());
	}

}

class Hub {
	private FakeApplier applier;

	public Hub(ArrayReader reader, FakeApplier applier) {
		this.applier = applier;
		reader.attach(this);
	}

	public void onInsert(Integer position, String data) {
		applier.insert(position, data);
	}
}

class ArrayReader {
	private Hub hub;
	private Integer position;

	public ArrayReader(Integer position, String namespace, String collection) {
		this.position = position;
	}

	public void attach(Hub hub) {
		this.hub = hub;
	}

	public void insert(String data) {
		hub.onInsert(++position, data);
	}
}

class FakeApplier {
	private Integer position;

	public Object lastPosition() {
		return position;
	}

	public void insert(Integer position, String data) {
		this.position = position;
	}	
}