package bumblebee.core.applier.rest;

import java.io.IOException;
import java.util.Map.Entry;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import bumblebee.core.events.Event;

public class JSONEventTranslator {
	public String toJson(Event event) {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();
			for (Entry<String, Object> e : event.getData().entrySet()) {
				builder.field(e.getKey(), e.getValue());
			}
			builder.endObject();
			return builder.string();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return null;
	}
}
