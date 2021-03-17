package org.apache.nifi.processors.fix.store;

import org.apache.nifi.components.state.StateManager;

import quickfix.MessageStore;
import quickfix.MessageStoreFactory;
import quickfix.SessionID;

public class NifiStateManagerStore implements MessageStoreFactory{

	private StateManager stateManager;
	
	public NifiStateManagerStore(StateManager stateManager) {
		this.stateManager = stateManager;
	}



	@Override
	public MessageStore create(SessionID sessionId) {
		try {
            return new StateManagerStore(stateManager);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}

}
