package bumblebee.core;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import bumblebee.core.aux.ArrayReader;
import bumblebee.core.aux.FakeApplier;


public class HubTest {

	@Test public void shouldReplicateInsert() {
		Map<String, Object> data1 = new HashMap<String, Object>();
		data1.put("id", 1);
		Map<String, Object> data2 = new HashMap<String, Object>();
		data2.put("id", 2);
		
		Integer position = 0;
		String namespace = "ns";
		String collection = "coll";
		
		ArrayReader arrayReader = new ArrayReader(position, namespace, collection);		
		FakeApplier applier = new FakeApplier();
		
		new Hub(arrayReader, applier);
		
		arrayReader.onInsert(data1);
		arrayReader.onInsert(data2);
		
		Integer expectedPosition = 2;
		assertEquals(expectedPosition, applier.lastPosition());
	}

}
