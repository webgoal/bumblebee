package bumblebee.core.exceptions;

public class BusinessException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public BusinessException(Throwable cause) {
		super(cause);
	}
}
