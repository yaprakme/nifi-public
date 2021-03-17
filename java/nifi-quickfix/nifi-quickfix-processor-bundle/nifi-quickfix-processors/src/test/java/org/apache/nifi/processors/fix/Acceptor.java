/*******************************************************************************
 * Copyright (c) quickfixengine.org  All rights reserved.
 *
 * This file is part of the QuickFIX FIX Engine
 *
 * This file may be distributed under the terms of the quickfixengine.org
 * license as defined by quickfixengine.org and appearing in the file
 * LICENSE included in the packaging of this file.
 *
 * This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
 * THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE.
 *
 * See http://www.quickfixengine.org/LICENSE for licensing information.
 *
 * Contact ask@quickfixengine.org if any conditions of this licensing
 * are not clear to you.
 ******************************************************************************/

package org.apache.nifi.processors.fix;

import static quickfix.Acceptor.SETTING_ACCEPTOR_TEMPLATE;
import static quickfix.Acceptor.SETTING_SOCKET_ACCEPT_ADDRESS;
import static quickfix.Acceptor.SETTING_SOCKET_ACCEPT_PORT;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.management.JMException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.DoNotSend;
import quickfix.FieldConvertError;
import quickfix.FieldNotFound;
import quickfix.FileStoreFactory;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.RejectLogon;
import quickfix.RuntimeError;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.SessionSettings;
import quickfix.SocketAcceptor;
import quickfix.StringField;
import quickfix.UnsupportedMessageType;
import quickfix.field.Account;
import quickfix.field.AvgPx;
import quickfix.field.BeginString;
import quickfix.field.CopyMsgIndicator;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecInst;
import quickfix.field.ExecRestatementReason;
import quickfix.field.ExecType;
import quickfix.field.ExpireDate;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.LeavesQty;
import quickfix.field.LegLastPx;
import quickfix.field.LegLastQty;
import quickfix.field.LegSecurityID;
import quickfix.field.LegSecurityIDSource;
import quickfix.field.LegSymbol;
import quickfix.field.ListID;
import quickfix.field.MatchIncrement;
import quickfix.field.MaxFloor;
import quickfix.field.MultiLegReportingType;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderCapacity;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.PartyID;
import quickfix.field.PartyIDSource;
import quickfix.field.PartyRole;
import quickfix.field.Price;
import quickfix.field.SecurityID;
import quickfix.field.SecurityIDSource;
import quickfix.field.SenderCompID;
import quickfix.field.SettlCurrAmt;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.field.TimeInForce;
import quickfix.field.TradeID;
import quickfix.field.TradeReportID;
import quickfix.field.TradingSessionID;
import quickfix.field.TransactTime;
import quickfix.field.TrdMatchID;
import quickfix.field.TriggerAction;
import quickfix.field.TriggerPrice;
import quickfix.field.TriggerPriceDirection;
import quickfix.field.TriggerPriceType;
import quickfix.field.TriggerSecurityID;
import quickfix.field.TriggerSecurityIDSource;
import quickfix.field.TriggerSymbol;
import quickfix.field.TriggerTradingSessionID;
import quickfix.field.TriggerType;
import quickfix.mina.acceptor.DynamicAcceptorSessionProvider;
import quickfix.mina.acceptor.DynamicAcceptorSessionProvider.TemplateMapping;

public class Acceptor {
    private final static Logger log = LoggerFactory.getLogger(Acceptor.class);
    private final SocketAcceptor acceptor;
    private final Map<InetSocketAddress, List<TemplateMapping>> dynamicSessionMappings = new HashMap<>();

    public Acceptor(SessionSettings settings) throws ConfigError, FieldConvertError, JMException {
        Application application = new Application(settings);
        MessageStoreFactory messageStoreFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory(true, true, true);
        MessageFactory messageFactory = new DefaultMessageFactory();

        acceptor = new SocketAcceptor(application, messageStoreFactory, settings, logFactory,
                messageFactory);

        configureDynamicSessions(settings, application, messageStoreFactory, logFactory,
                messageFactory);
    }

    private void configureDynamicSessions(SessionSettings settings, Application application,
            MessageStoreFactory messageStoreFactory, LogFactory logFactory,
            MessageFactory messageFactory) throws ConfigError, FieldConvertError {
        //
        // If a session template is detected in the settings, then
        // set up a dynamic session provider.
        //

        Iterator<SessionID> sectionIterator = settings.sectionIterator();
        while (sectionIterator.hasNext()) {
            SessionID sessionID = sectionIterator.next();
            if (isSessionTemplate(settings, sessionID)) {
                InetSocketAddress address = getAcceptorSocketAddress(settings, sessionID);
                getMappings(address).add(new TemplateMapping(sessionID, sessionID));
            }
        }

        for (Map.Entry<InetSocketAddress, List<TemplateMapping>> entry : dynamicSessionMappings
                .entrySet()) {
            acceptor.setSessionProvider(entry.getKey(), new DynamicAcceptorSessionProvider(
                    settings, entry.getValue(), application, messageStoreFactory, logFactory,
                    messageFactory));
        }
    }

    private List<TemplateMapping> getMappings(InetSocketAddress address) {
        return dynamicSessionMappings.computeIfAbsent(address, k -> new ArrayList<>());
    }

    private InetSocketAddress getAcceptorSocketAddress(SessionSettings settings, SessionID sessionID)
            throws ConfigError, FieldConvertError {
        String acceptorHost = "0.0.0.0";
        if (settings.isSetting(sessionID, SETTING_SOCKET_ACCEPT_ADDRESS)) {
            acceptorHost = settings.getString(sessionID, SETTING_SOCKET_ACCEPT_ADDRESS);
        }
        int acceptorPort = (int) settings.getLong(sessionID, SETTING_SOCKET_ACCEPT_PORT);

        return new InetSocketAddress(acceptorHost, acceptorPort);
    }

    private boolean isSessionTemplate(SessionSettings settings, SessionID sessionID)
            throws ConfigError, FieldConvertError {
        return settings.isSetting(sessionID, SETTING_ACCEPTOR_TEMPLATE)
                && settings.getBool(sessionID, SETTING_ACCEPTOR_TEMPLATE);
    }

    private void start() throws RuntimeError, ConfigError {
        acceptor.start();
    }

    private void stop() {
        acceptor.stop();
    }

    public static void main(String[] args) throws Exception {
        try {
            InputStream inputStream = getSettingsInputStream(args);
            SessionSettings settings = new SessionSettings(inputStream);
            inputStream.close();

            Acceptor executor = new Acceptor(settings);
            executor.start();
            
            log.debug("Accepting initiators...");
            log.debug("Enter q to quit");
            log.debug("Enter 1 to send 1  execution report");
            log.debug("Enter 2 to send 100  execution report");
            log.debug("Enter 3 to send 1 trade capture report");

            while(true) {
            	
            	 char letter = (char) System.in.read();
            	 if (letter == 'q') {
            		 executor.stop();
            		 break;
            	 }else if (letter == '1') {
            		 log.debug("Sending an execution report");
            		 sendExecutionReport("order");
            	 }else if (letter == '2') {
            		 log.debug("Sending 100 count execution report");
            		 for (int i = 0; i < 100; i++) {
            			 sendExecutionReport("order"+i);
					 }
            	 }else if (letter == '3') {
            	     log.debug("Sending a trade capture report");
            	     sendTradeCaptureReport();
                 }
            }
            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    
  static void sendTradeCaptureReport() throws Exception {
    	
	   quickfix.fix50sp2.TradeCaptureReport tcr = new quickfix.fix50sp2.TradeCaptureReport(new LastQty(1), new LastPx(1));
	  
	   tcr.set(new Symbol("ISC"));
	   tcr.set(new TransactTime(LocalDateTime.now(ZoneId.of("UTC"))));
	   tcr.setField(new StringField(70, "allocID"));
	   tcr.set(new TradeReportID("TradeReportID"));
	   tcr.set(new TradeID("TradeID"));
	  
	   quickfix.fix50sp2.TradeCaptureReport.NoSides noSidesGroup = new quickfix.fix50sp2.TradeCaptureReport.NoSides();
	   noSidesGroup.set(new Side('2'));
	   tcr.addGroup(noSidesGroup);
	   
       send("FIXT.1.1", "BANZAI", "NIFI", tcr);   
    }
    
    
    
    static void sendExecutionReport(String orderid) throws Exception{
        send("FIXT.1.1", "BANZAI", "NIFI", getExecutionReport(orderid));    
    }
    
    static quickfix.fix50sp2.ExecutionReport getExecutionReport(String orderid) throws Exception{
    	OrderQty orderQty = new OrderQty(1.2);
    	quickfix.fix50sp2.ExecutionReport executionReport = new quickfix.fix50sp2.ExecutionReport(
    			new OrderID(orderid), new ExecID("execid"), new ExecType('0'), new OrdStatus(
                        OrdStatus.FILLED), new Side('1'), new LeavesQty(0), new CumQty(0));

      
        executionReport.set(orderQty);
        executionReport.set(new LastQty(1));
        executionReport.set(new LastPx(1));
        executionReport.set(new AvgPx(1));
        
        
       
        
        quickfix.fix50sp2.ExecutionReport.NoPartyIDs noPartyIDsGroup = new quickfix.fix50sp2.ExecutionReport.NoPartyIDs();
        noPartyIDsGroup.set(new PartyID("partyId"));
        noPartyIDsGroup.set(new PartyIDSource('D'));
        noPartyIDsGroup.set(new PartyRole(1));
        executionReport.addGroup(noPartyIDsGroup);
        noPartyIDsGroup.set(new PartyID("partyId2"));
        noPartyIDsGroup.set(new PartyIDSource('D'));
        noPartyIDsGroup.set(new PartyRole(2));
        executionReport.addGroup(noPartyIDsGroup);
        
        executionReport.set(new ListID("listId"));
        executionReport.setField(new TrdMatchID("tradeMatchId"));
        executionReport.setField(new StringField(20034, "comboMatchID"));
        executionReport.setField(new Account("account"));
        
        // instrument 
        executionReport.set(new Symbol("ISC"));
        executionReport.set(new SecurityID("CUSIP"));
        executionReport.set(new SecurityIDSource("1"));
        
        executionReport.set(new OrdType('1'));
        executionReport.set(new Price(12.34));
        executionReport.setField(new StringField(21100, "lastUnderlyingPx"));
        executionReport.setField(new StringField(21101, "underlyingColPx"));
        
        //TriggeringInstruction/
        executionReport.set(new TriggerType('1'));
        executionReport.set(new TriggerAction('1'));
        executionReport.set(new TriggerPrice(12.4));
        executionReport.set(new TriggerTradingSessionID("1"));
        executionReport.set(new TriggerSymbol("triggerSymbol"));
        executionReport.set(new TriggerSecurityID("triggerSecurityID"));
        executionReport.set(new TriggerSecurityIDSource("M"));
        executionReport.set(new TriggerPriceType('1'));
        executionReport.set(new TriggerPriceDirection('U'));
        
        executionReport.set(new TimeInForce('0'));
        executionReport.set(new TradingSessionID("1"));
        executionReport.set(new ExpireDate("expireDate"));
        executionReport.set(new ExecInst("execInst"));
        executionReport.set(new OrderCapacity('A'));
        
        executionReport.set(new TransactTime(LocalDateTime.now(ZoneId.of("UTC"))));
        executionReport.set(new ExecRestatementReason(1));
   
        
        executionReport.set(new MultiLegReportingType('3'));
        executionReport.setField(new StringField(70, "allocID"));
        executionReport.set(new CopyMsgIndicator(true));  
        executionReport.set(new MatchIncrement(4.5));  
        executionReport.set(new MaxFloor(3.4));  
        executionReport.set(new SettlCurrAmt(3.45));  

        executionReport.setField(new StringField(20015, "optPremiumAmt"));
        executionReport.setField(new StringField(20009, "orderReference"));
        executionReport.setField(new StringField(20199, "openCloseIndicator"));
        
        
        
        quickfix.fix50sp2.ExecutionReport.NoLegs noLegsGroup = new quickfix.fix50sp2.ExecutionReport.NoLegs();
        noLegsGroup.set(new LegSymbol("legSymbol"));
        noLegsGroup.set(new LegSecurityID("legSecurityID"));
        noLegsGroup.set(new LegSecurityIDSource("legSecurityIDSource"));
        noLegsGroup.set(new LegLastPx(7.8));
        noLegsGroup.set(new LegLastQty(0.89));
        noLegsGroup.setField(new StringField(20200, "legTrdMatchID"));
        executionReport.addGroup(noLegsGroup);
        
        return executionReport;
        
    }
    
    
    static void send(String beginString, String senderCompId, String targetCompId, Message message) throws Exception{
    	SessionID sessionID =  new SessionID(new BeginString(beginString), new SenderCompID(senderCompId), new TargetCompID(targetCompId));
        Session session = Session.lookupSession(sessionID);
        if (session == null) {
            throw new SessionNotFound(sessionID.toString());
        } 
        session.send(message);	
    }
    
    
    private static InputStream getSettingsInputStream(String[] args) throws FileNotFoundException {
        InputStream inputStream = null;
        if (args.length == 0) {
            inputStream = Acceptor.class.getResourceAsStream("executor.cfg");
        } else if (args.length == 1) {
            inputStream = new FileInputStream(args[0]);
        }
        if (inputStream == null) {
            System.out.println("usage: " + Acceptor.class.getName() + " [configFile].");
            System.exit(1);
        }
        return inputStream;
    }
}

class Application extends quickfix.MessageCracker implements quickfix.Application {
    public Application(SessionSettings settings) throws ConfigError, FieldConvertError {   }
    public void onCreate(SessionID sessionID) {}
    public void onLogon(SessionID sessionID) {}
    public void onLogout(SessionID sessionID) {}
    public void toAdmin(quickfix.Message message, SessionID sessionID) {}
    public void toApp(quickfix.Message message, SessionID sessionID) throws DoNotSend {}
    public void fromAdmin(quickfix.Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat,
            IncorrectTagValue, RejectLogon {}
    public void fromApp(quickfix.Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat,
            IncorrectTagValue, UnsupportedMessageType { }
}
