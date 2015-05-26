package bumblebee.core.reader;

import java.util.Map;

public interface Reader {
	public void onInsert(Map<String, Object> data);
}
