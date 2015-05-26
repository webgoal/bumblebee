package bumblebee.core;

class Hub {
	public Hub(ArrayReader reader, FakeApplier applier) {
		reader.attach(applier);
	}
}