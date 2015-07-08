package bumblebee.core.events;


public class UpdateEvent extends Event {
	@Override public boolean isUpdate() {
		return true;
	}
}
