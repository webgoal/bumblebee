package bumblebee.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class HubTest {

	@Test public void shouldReplicateInsert() {
		Integer position = 0;
		String namespace = "ns";
		String collection = "coll";
		
		ArrayReader arrayReader = new ArrayReader(position, namespace, collection);
		
		FakeApplier applier = new FakeApplier();
		
		new Hub(arrayReader, applier);
		
		arrayReader.onInsert("first data");
		arrayReader.onInsert("second data");
		
		Integer expectedPosition = 2;
		assertEquals(expectedPosition, applier.lastPosition());
	}

}

class Hub {
	public Hub(ArrayReader reader, FakeApplier applier) {
		reader.attach(applier);
	}
}

class ArrayReader {
	private Integer position;
	private FakeApplier applier;

	public ArrayReader(Integer position, String namespace, String collection) {
		this.position = position;
	}

	public void attach(FakeApplier applier) {
		this.applier = applier;
	}

	public void onInsert(String data) {
		applier.insert(++position, data);
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