package bumblebee.core.interfaces;

import java.util.Map;

public interface Consumer {
	void insert(Integer integer, Map<String, Object> data);
}
