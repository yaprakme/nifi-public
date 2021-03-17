package org.apache.nifi.processors.fix.store;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;

import quickfix.MemoryStore;
import quickfix.MessageStore;

public class StateManagerStore implements MessageStore{

	private final MemoryStore cache;
	private StateManager stateManager;
	

	private final String TARGET_MSG_SEQ_NUM = "targetMsgSeqNum"; 
	private final String SENDER_MSG_SEQ_NUM = "senderMsgSeqNum"; 
	
	public StateManagerStore(StateManager stateManager) throws Exception {
		this.stateManager = stateManager;
		this.cache = new MemoryStore();
		
		loadCache();
	}

	private void loadCache() throws Exception {
		final StateMap stateMap = stateManager.getState(Scope.CLUSTER); 
		String targetMsgSeqNum = stateMap.get(TARGET_MSG_SEQ_NUM); 
		String senderMsgSeqNum = stateMap.get(SENDER_MSG_SEQ_NUM); 
		
		if (targetMsgSeqNum != null && senderMsgSeqNum != null) {
			cache.setNextTargetMsgSeqNum(Integer.parseInt(targetMsgSeqNum));
			cache.setNextSenderMsgSeqNum(Integer.parseInt(senderMsgSeqNum));
		}else {
			storeSeqs();
		}
            
    }
	
	public void storeSeqs() throws IOException {
		final Map<String, String> newState = new HashMap<>();
     	newState.put(TARGET_MSG_SEQ_NUM,  ""+ cache.getNextTargetMsgSeqNum());
     	newState.put(SENDER_MSG_SEQ_NUM,  ""+ cache.getNextSenderMsgSeqNum());
        stateManager.setState(newState, Scope.CLUSTER);
	}
	
	@Override
	public void get(int arg0, int arg1, Collection<String> arg2) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Date getCreationTime() throws IOException {
		// TODO Auto-generated method stub
		return new Date();
	}

	@Override
	public int getNextSenderMsgSeqNum() throws IOException {
		return cache.getNextSenderMsgSeqNum();
	}

	@Override
	public int getNextTargetMsgSeqNum() throws IOException {
		return cache.getNextTargetMsgSeqNum();
	}

	@Override
	public void incrNextSenderMsgSeqNum() throws IOException {
		 cache.incrNextSenderMsgSeqNum();		
		 storeSeqs();
	}

	@Override
	public void incrNextTargetMsgSeqNum() throws IOException {
		 cache.incrNextTargetMsgSeqNum();
		 storeSeqs();
	}

	@Override
	public void refresh() throws IOException {
		try {
            loadCache();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
	}

	@Override
	public void reset() throws IOException {
		cache.reset();
		storeSeqs();
	}

	@Override
	public boolean set(int arg0, String arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setNextSenderMsgSeqNum(int next) throws IOException {
		cache.setNextSenderMsgSeqNum(next);
		storeSeqs();
	}

	@Override
	public void setNextTargetMsgSeqNum(int next) throws IOException {
		cache.setNextTargetMsgSeqNum(next);	
		storeSeqs();
	}

}
