package org.apache.nifi.processors.fix.handler;

public class MessageHandlerException extends Exception { 
	private static final long serialVersionUID = 1L;
	private String errorCode;
    public MessageHandlerException(String errorCode, String errorMessage) {
        super(errorMessage);
        this.errorCode = errorCode;
    }
	public String getErrorCode() {
		return errorCode;
	}
}