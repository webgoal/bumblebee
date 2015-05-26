package bumblebee.core.interfaces;

import java.util.Map;

public interface Producer {
	void attach(Consumer consumer);
	void onInsert(Map<String, Object> data);
}
