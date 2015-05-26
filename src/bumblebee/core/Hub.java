package bumblebee.core;

import bumblebee.core.interfaces.Consumer;
import bumblebee.core.interfaces.Producer;

class Hub {
	public Hub(Producer producer, Consumer consumer) {
		producer.attach(consumer);
	}
}