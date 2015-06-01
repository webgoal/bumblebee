package bumblebee.core;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import bumblebee.core.aux.ArrayProducer;
import bumblebee.core.aux.DummyConsumer;
import bumblebee.core.exceptions.BusinessException;


public class HubTest {

	@Test public void shouldReplicateInsert() throws BusinessException {
		Map<String, Object> data1 = new HashMap<String, Object>();
		data1.put("id", 1);
		Map<String, Object> data2 = new HashMap<String, Object>();
		data2.put("id", 2);
		
		Long position = 0L;
		String namespace = "ns";
		String collection = "coll";
		
		ArrayProducer arrayReader = new ArrayProducer(position, namespace, collection);		
		DummyConsumer applier = new DummyConsumer();
		
		new Hub(arrayReader, applier);
		
		arrayReader.doInsert(data1);
		arrayReader.doInsert(data2);
		
		Long expectedPosition = 2L;
		assertEquals(expectedPosition, applier.lastPosition());
	}

}
