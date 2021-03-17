

package org.apache.nifi.processors.fix.handler;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.nifi.logging.ComponentLog;

import quickfix.Application;
import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.MsgType;

public class DefaultApplication implements Application {
    
	private String user;
	private String pass;
	private String reset;
	private ComponentLog logger;
	private LinkedBlockingQueue<Message> queue;
	

	public DefaultApplication(String user, String pass, String reset, ComponentLog logger, LinkedBlockingQueue<Message>queue) {
		super();
		this.user = user;
		this.pass = pass;
		this.reset = reset;
		this.logger = logger;
		this.queue = queue;
	}

	public void onCreate(SessionID sessionID) {
    }

    public void onLogon(SessionID sessionID) {
    	logger.debug("ONLOGON-SESSIONID:"+sessionID);
    }

    public void onLogout(SessionID sessionID) {
    	logger.debug("ONLOGOUT-SESSIONID:"+sessionID);
    }

    public void toAdmin(quickfix.Message message, SessionID sessionID) {
    	try {
			final String msgType = message.getHeader().getString(MsgType.FIELD);
			if(MsgType.LOGON.compareTo(msgType) == 0)
			{
				if (user != null) {
				  message.setString(quickfix.field.Username.FIELD, user);
				}
				if (pass != null) {
					message.setString(quickfix.field.Password.FIELD, pass);
				}
			    if (reset != null && reset.equals("Y")) {
			    	message.setString(quickfix.field.ResetSeqNumFlag.FIELD, reset);
			    }
			}
			
			if (logger.isDebugEnabled()) {
				logger.debug("TOADMIN-MESSAGE:"+message.toXML());
			}
			
		} catch (Exception e) {
			logger.error("TOADMIN-ERROR:",e);
		}
    }

    public void toApp(quickfix.Message message, SessionID sessionID) throws DoNotSend {
    	if (logger.isDebugEnabled()) {
			logger.debug("TOAPP-MESSAGE:"+message.toXML());
		}
    }

    public void fromAdmin(quickfix.Message message, SessionID sessionID) throws FieldNotFound,
            IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    	 if (logger.isDebugEnabled()) {
	     	logger.debug("FROMADMIN-MESSAGE:"+message.toXML());
		 }
    	
    	 
    	 try {
         	
         	queue.put(message);
           
         } catch (Exception e) {
         	logger.error("fromAdmin", e);
         }
         
    	 
    }

    public void fromApp(quickfix.Message message, SessionID sessionID) throws FieldNotFound,
            IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
    	if (logger.isDebugEnabled()) {
			logger.debug("FROMAPP-MESSAGE:"+message.toXML());
		}
    	
    	
        try {
        	
        	queue.put(message);
          
        } catch (Exception e) {
        	logger.error("fromApp", e);
        }
        
    }


   
}
