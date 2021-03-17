package org.apache.nifi.processors.fix.handler;

import quickfix.Message;

public interface MessageHandlerImpl {
	public static final String SEQUENCE_EXPIRY = "sequenceExpiry"; 
	public static final String INVALID_LOGIN = "LogonException"; 
	
	
	public String parse(Message message) throws Exception;
}
