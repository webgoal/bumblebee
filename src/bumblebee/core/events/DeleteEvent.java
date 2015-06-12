package bumblebee.core.events;


public class DeleteEvent extends Event {
	@Override public boolean isDelete() {
		return true;
	}
}
