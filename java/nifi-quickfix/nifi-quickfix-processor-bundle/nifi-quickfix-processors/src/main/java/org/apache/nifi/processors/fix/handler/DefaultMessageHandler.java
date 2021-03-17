package org.apache.nifi.processors.fix.handler;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.fasterxml.jackson.databind.ObjectMapper;

import quickfix.Message;
import quickfix.StringField;
import quickfix.field.Account;
import quickfix.field.AllocID;
import quickfix.field.AvgPx;
import quickfix.field.BidPx;
import quickfix.field.BidSize;
import quickfix.field.ClearingBusinessDate;
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
import quickfix.field.MatchStatus;
import quickfix.field.MaxFloor;
import quickfix.field.MsgSeqNum;
import quickfix.field.MsgType;
import quickfix.field.MultiLegReportingType;
import quickfix.field.NoLegs;
import quickfix.field.NoPartyIDs;
import quickfix.field.NoSides;
import quickfix.field.OfferPx;
import quickfix.field.OfferSize;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderCapacity;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.OrigTradeID;
import quickfix.field.PreviouslyReported;
import quickfix.field.Price;
import quickfix.field.QuoteCancelType;
import quickfix.field.QuoteID;
import quickfix.field.QuoteStatus;
import quickfix.field.QuoteType;
import quickfix.field.SecondaryTradeReportID;
import quickfix.field.SecondaryTrdType;
import quickfix.field.SecurityID;
import quickfix.field.SecurityIDSource;
import quickfix.field.SendingTime;
import quickfix.field.SettlCurrAmt;
import quickfix.field.SettlDate;
import quickfix.field.SettlPrice;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TimeInForce;
import quickfix.field.TradeDate;
import quickfix.field.TradeID;
import quickfix.field.TradeReportID;
import quickfix.field.TradeReportRefID;
import quickfix.field.TradeReportTransType;
import quickfix.field.TradeReportType;
import quickfix.field.TradingSessionID;
import quickfix.field.TransactTime;
import quickfix.field.TrdMatchID;
import quickfix.field.TrdSubType;
import quickfix.field.TrdType;
import quickfix.field.TriggerAction;
import quickfix.field.TriggerPrice;
import quickfix.field.TriggerPriceDirection;
import quickfix.field.TriggerPriceType;
import quickfix.field.TriggerSecurityID;
import quickfix.field.TriggerSecurityIDSource;
import quickfix.field.TriggerSymbol;
import quickfix.field.TriggerTradingSessionID;
import quickfix.field.TriggerType;

public class DefaultMessageHandler implements MessageHandlerImpl {
    
	private final ObjectMapper mapper = new ObjectMapper();
	
	private long timeZoneOffset = TimeZone.getDefault().getRawOffset() / (1000 * 60 * 60);
    private DateTimeFormatter basicFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    private DateTimeFormatter basicDayFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	
	public String parse(Message message) throws Exception {
		String msgType = message.getHeader().getString(MsgType.FIELD);
		
		if (msgType.equals(MsgType.EXECUTION_REPORT)) {
			return handleExecutionReport(message);
		}else if (msgType.equals(MsgType.TRADE_CAPTURE_REPORT)) {
			return handleTradeCaptureReport(message);
		}else if (msgType.equals(MsgType.QUOTE_STATUS_REPORT)) {
			return handleQuoteStatusReport(message);
		}else if (msgType.equals(MsgType.LOGOUT)) {
			Text text = new Text();
			if (message.isSetField(text)) {
				throw new MessageHandlerException(INVALID_LOGIN, message.getField(text).getValue());
			}
		}
		return null;
	}


	public String handleExecutionReport(Message message)  throws Exception{
		Map<String, Object> node = new LinkedHashMap<String, Object>();
		List<Object> groupList = null;
		int groupCount = 0;
		
		node.put("MsgType", MsgType.EXECUTION_REPORT);
		
		OrderID orderID = new OrderID();
		String orderId = message.isSetField(orderID) ? message.getField(orderID).getValue() : null;
		node.put("OrderID", orderId);
		
		ExecID execID = new ExecID();
		node.put("ExecID",  message.isSetField(execID) ? message.getField(execID).getValue() : null);
		
		ListID listID = new ListID();
		node.put("ListID",  message.isSetField(listID) ? message.getField(listID).getValue() : null);
		
		TrdMatchID trdMatchID = new TrdMatchID();
		node.put("TrdMatchID",  message.isSetField(trdMatchID) ? message.getField(trdMatchID).getValue() : null);

		//nasdaq extension
		StringField comboMatchID = new StringField(20034);
		node.put("ComboMatchID",  message.isSetField(comboMatchID) ? message.getField(comboMatchID).getValue() : null);
		
		ExecType execType = new ExecType();
		node.put("ExecType",  message.isSetField(execType) ? message.getField(execType).getValue() : null);
		
		OrdStatus ordStatus = new OrdStatus();
		node.put("OrdStatus",  message.isSetField(ordStatus) ? message.getField(ordStatus).getValue() : null);

		Account account = new Account();
		node.put("Account",  message.isSetField(account) ? message.getField(account).getValue() : null);

		//instrument
		putInstrument(node, message);
		
		Side side = new Side();
		node.put("Side",  message.isSetField(side) ? message.getField(side).getValue() : null);
		
		OrderQty orderQty = new OrderQty();
		node.put("OrderQty",  message.isSetField(orderQty) ? message.getField(orderQty).getValue() : null);
		
		OrdType ordType = new OrdType();
		node.put("OrdType",  message.isSetField(ordType) ? message.getField(ordType).getValue() : null);
		
		Price price = new Price();
		node.put("Price",  message.isSetField(price) ? message.getField(price).getValue() : null);

		//nasdaq extension
		StringField lastUnderlyingPx = new StringField(21100);
		node.put("LastUnderlyingPx",  message.isSetField(lastUnderlyingPx) ? message.getField(lastUnderlyingPx).getValue() : null);
		StringField underlyingColPx = new StringField(21101);
		node.put("UnderlyingColPx",  message.isSetField(underlyingColPx) ? message.getField(underlyingColPx).getValue() : null);
		
		//TriggeringInstruction
		TriggerType triggerType = new TriggerType();
		node.put("TriggerType",  message.isSetField(triggerType) ? message.getField(triggerType).getValue() : null);
        TriggerAction triggerAction = new TriggerAction();
		node.put("TriggerAction",  message.isSetField(triggerAction) ? message.getField(triggerAction).getValue() : null);
        TriggerPrice triggerPrice = new TriggerPrice();
		node.put("TriggerPrice",  message.isSetField(triggerPrice) ? message.getField(triggerPrice).getValue() : null);
        TriggerTradingSessionID triggerTradingSessionID = new TriggerTradingSessionID();
		node.put("TriggerTradingSessionID",  message.isSetField(triggerTradingSessionID) ? message.getField(triggerTradingSessionID).getValue() : null);
		TriggerSymbol triggerSymbol = new TriggerSymbol();
		node.put("TriggerSymbol",  message.isSetField(triggerSymbol) ? message.getField(triggerSymbol).getValue() : null);
		TriggerSecurityID triggerSecurityID = new TriggerSecurityID();
		node.put("triggerSecurityID",  message.isSetField(triggerSecurityID) ? message.getField(triggerSecurityID).getValue() : null);
        TriggerSecurityIDSource triggerSecurityIDSource = new TriggerSecurityIDSource();
		node.put("TriggerSecurityIDSource",  message.isSetField(triggerSecurityIDSource) ? message.getField(triggerSecurityIDSource).getValue() : null);
		TriggerPriceType triggerPriceType = new TriggerPriceType();
		node.put("TriggerPriceType",  message.isSetField(triggerPriceType) ? message.getField(triggerPriceType).getValue() : null);
		TriggerPriceDirection triggerPriceDirection = new TriggerPriceDirection();
		node.put("TriggerPriceDirection",  message.isSetField(triggerPriceDirection) ? message.getField(triggerPriceDirection).getValue() : null);

		TimeInForce timeInForce = new TimeInForce();
		node.put("TimeInForce",  message.isSetField(timeInForce) ? message.getField(timeInForce).getValue() : null);

		TradingSessionID tradingSessionID = new TradingSessionID();
		node.put("TradingSessionID",  message.isSetField(tradingSessionID) ? message.getField(tradingSessionID).getValue() : null);
		
		ExpireDate expireDate = new ExpireDate();
		node.put("ExpireDate",  message.isSetField(expireDate) ? message.getField(expireDate).getValue() : null);

		ExecInst execInst = new ExecInst();
		node.put("ExecInst",  message.isSetField(execInst) ? message.getField(execInst).getValue() : null);
		
		OrderCapacity orderCapacity = new OrderCapacity();
		node.put("OrderCapacity",  message.isSetField(orderCapacity) ? message.getField(orderCapacity).getValue() : null);

        LeavesQty leavesQty = new LeavesQty();
		node.put("LeavesQty",  message.isSetField(leavesQty) ? message.getField(leavesQty).getValue() : null);
		
		CumQty cumQty = new CumQty();
		node.put("CumQty",  message.isSetField(cumQty) ? message.getField(cumQty).getValue() : null);
		
		AvgPx avgPx = new AvgPx();
		node.put("AvgPx",  message.isSetField(avgPx) ? message.getField(avgPx).getValue() : null);
		
		putTransactTime(node, message);
		
        ExecRestatementReason execRestatementReason = new ExecRestatementReason();
		node.put("ExecRestatementReason",  message.isSetField(execRestatementReason) ? message.getField(execRestatementReason).getValue() : null);

        MultiLegReportingType multiLegReportingType = new MultiLegReportingType();
		node.put("MultiLegReportingType",  message.isSetField(multiLegReportingType) ? message.getField(multiLegReportingType).getValue() : null);

        

		// NoLegs group
		LegSymbol legSymbol = new LegSymbol();
		LegSecurityID legSecurityID = new LegSecurityID();
		LegSecurityIDSource legSecurityIDSource = new LegSecurityIDSource();
		LegLastPx legLastPx = new LegLastPx();
		LegLastQty legLastQty = new LegLastQty();
		StringField legTrdMatchID = new StringField(20200); // nasdaq extension
		quickfix.fix50sp2.ExecutionReport.NoLegs noLegsGroup = new quickfix.fix50sp2.ExecutionReport.NoLegs();
		groupList = new ArrayList<Object>();  
		groupCount = message.getGroupCount(NoLegs.FIELD);
		for (int i=0; i<groupCount; i++) {
			Map<String, Object> noLegsNode = new LinkedHashMap<String, Object>();
			message.getGroup(i + 1, noLegsGroup);
			noLegsNode.put("LegSymbol",  noLegsGroup.isSet(legSymbol) ? noLegsGroup.get(legSymbol).getValue() : null);
			noLegsNode.put("LegSecurityID",  noLegsGroup.isSet(legSecurityID) ? noLegsGroup.get(legSecurityID).getValue() : null);
			noLegsNode.put("LegSecurityIDSource",  noLegsGroup.isSet(legSecurityIDSource) ? noLegsGroup.get(legSecurityIDSource).getValue() : null);
			noLegsNode.put("LegLastPx",  noLegsGroup.isSet(legLastPx) ? noLegsGroup.get(legLastPx).getValue() : null);
			noLegsNode.put("LegLastQty",  noLegsGroup.isSet(legLastQty) ? noLegsGroup.get(legLastQty).getValue() : null);
			noLegsNode.put("LegTrdMatchID",  noLegsGroup.isSetField(legTrdMatchID) ? noLegsGroup.getField(legTrdMatchID).getValue() : null);
			groupList.add(noLegsNode);
		}
		node.put("NoLegs", groupList);
		
		
		
		AllocID allocID = new AllocID();
		node.put("AllocID",  message.isSetField(allocID) ? message.getField(allocID).getValue() : null);
		
		CopyMsgIndicator copyMsgIndicator = new CopyMsgIndicator();
		node.put("CopyMsgIndicator",  message.isSetField(copyMsgIndicator) ? message.getField(copyMsgIndicator).getValue() : null);

		MatchIncrement matchIncrement = new MatchIncrement();
		node.put("MatchIncrement",  message.isSetField(matchIncrement) ? message.getField(matchIncrement).getValue() : null);
		
		MaxFloor maxFloor = new MaxFloor();
		node.put("MaxFloor",  message.isSetField(maxFloor) ? message.getField(maxFloor).getValue() : null);

        SettlCurrAmt settlCurrAmt = new SettlCurrAmt();
		node.put("SettlCurrAmt",  message.isSetField(settlCurrAmt) ? message.getField(settlCurrAmt).getValue() : null);

		//nasdaq extension
		StringField optPremiumAmt = new StringField(20015);
		node.put("OptPremiumAmt",  message.isSetField(optPremiumAmt) ? message.getField(optPremiumAmt).getValue() : null);
		StringField orderReference = new StringField(20009);
		node.put("OrderReference",  message.isSetField(orderReference) ? message.getField(orderReference).getValue() : null);
		StringField openCloseIndicator = new StringField(20199);
		node.put("OpenCloseIndicator",  message.isSetField(openCloseIndicator) ? message.getField(openCloseIndicator).getValue() : null);

		// put some log
		LocalDateTime now = LocalDateTime.now();
		node.put("MessageDropTime", now.toString());
		
		MsgSeqNum messageSeqNum = new MsgSeqNum();
		int messageSeqNumber = message.getHeader().getField(messageSeqNum).getValue();
		node.put("Id", messageSeqNumber + ":"+ orderId);
		
		return mapper.writeValueAsString(node);
	}
	
	
	public String handleQuoteStatusReport(Message message)  throws Exception{
		Map<String, Object> node = new LinkedHashMap<String, Object>();
		
		node.put("MsgType", MsgType.QUOTE_STATUS_REPORT);
		
		QuoteID quoteID = new QuoteID();
		String quoteId = message.isSetField(quoteID) ? message.getField(quoteID).getValue() : null;
		node.put("QuoteID",  quoteId);
		
		QuoteType quoteType = new QuoteType();
		node.put("QuoteType",  message.isSetField(quoteType) ? message.getField(quoteType).getValue() : null);
		
		QuoteCancelType quoteCancelType = new QuoteCancelType();
		node.put("QuoteCancelType",  message.isSetField(quoteCancelType) ? message.getField(quoteCancelType).getValue() : null);
		
		putInstrument(node, message);
		
		BidPx bidPx = new BidPx();
		node.put("BidPx",  message.isSetField(bidPx) ? message.getField(bidPx).getValue() : null);

		OfferPx offerPx = new OfferPx();
		node.put("OfferPx",  message.isSetField(offerPx) ? message.getField(offerPx).getValue() : null);

		BidSize bidSize = new BidSize();
		node.put("BidSize",  message.isSetField(bidSize) ? message.getField(bidSize).getValue() : null);

		OfferSize offerSize = new OfferSize();
		node.put("OfferSize",  message.isSetField(offerSize) ? message.getField(offerSize).getValue() : null);

		QuoteStatus quoteStatus = new QuoteStatus();
		node.put("QuoteStatus",  message.isSetField(quoteStatus) ? message.getField(quoteStatus).getValue() : null);

		Account account = new Account();
		node.put("Account",  message.isSetField(account) ? message.getField(account).getValue() : null);

		OrderCapacity orderCapacity = new OrderCapacity();
		node.put("OrderCapacity",  message.isSetField(orderCapacity) ? message.getField(orderCapacity).getValue() : null);

		CopyMsgIndicator copyMsgIndicator = new CopyMsgIndicator();
		node.put("CopyMsgIndicator",  message.isSetField(copyMsgIndicator) ? message.getField(copyMsgIndicator).getValue() : null);
		
		SendingTime sendingTime = new SendingTime();
		node.put("TransactTime",  message.getHeader().isSetField(sendingTime) ? message.getHeader().getField(sendingTime).getValue().toString() : null);
		
		// put some log
		LocalDateTime now = LocalDateTime.now();
		node.put("MessageDropTime", now.toString());
		
		MsgSeqNum messageSeqNum = new MsgSeqNum();
		int messageSeqNumber = message.getHeader().getField(messageSeqNum).getValue();
		node.put("Id", messageSeqNumber + ":"+ quoteId);
				
		return mapper.writeValueAsString(node);
	}
	
	public String handleTradeCaptureReport(Message message)  throws Exception{
		Map<String, Object> node = new LinkedHashMap<String, Object>();
		
		node.put("MsgType", MsgType.TRADE_CAPTURE_REPORT);
		
		TradeReportID tradeReportID = new TradeReportID();
		node.put("TradeReportID",  message.isSetField(tradeReportID) ? message.getField(tradeReportID).getValue() : null);
		
		TradeID tradeID = new TradeID();
		String trdID = message.isSetField(tradeID) ? message.getField(tradeID).getValue() : null;
		node.put("TradeID",  trdID);
		
		OrigTradeID origTradeID = new OrigTradeID();
		node.put("OrigTradeID",  message.isSetField(origTradeID) ? message.getField(origTradeID).getValue() : null);
		
		TradeReportRefID tradeReportRefID = new TradeReportRefID();
		node.put("TradeReportRefID",  message.isSetField(tradeReportRefID) ? message.getField(tradeReportRefID).getValue() : null);
		
		SecondaryTradeReportID secondaryTradeReportID = new SecondaryTradeReportID();
		node.put("SecondaryTradeReportID",  message.isSetField(secondaryTradeReportID) ? message.getField(secondaryTradeReportID).getValue() : null);
		
		TradeReportTransType tradeReportTransType = new TradeReportTransType();
		node.put("TradeReportTransType",  message.isSetField(tradeReportTransType) ? message.getField(tradeReportTransType).getValue() : null);
		
		TradeReportType tradeReportType = new TradeReportType();
		node.put("TradeReportType",  message.isSetField(tradeReportType) ? message.getField(tradeReportType).getValue() : null);
		
		TrdType trdType = new TrdType();
		node.put("TrdType",  message.isSetField(trdType) ? message.getField(trdType).getValue() : null);
		
		TrdSubType trdSubType = new TrdSubType();
		node.put("TrdSubType",  message.isSetField(trdSubType) ? message.getField(trdSubType).getValue() : null);
		
		MatchStatus matchStatus = new MatchStatus();
		node.put("MatchStatus",  message.isSetField(matchStatus) ? message.getField(matchStatus).getValue() : null);
		
		TrdMatchID trdMatchID = new TrdMatchID();
		node.put("TrdMatchID",  message.isSetField(trdMatchID) ? message.getField(trdMatchID).getValue() : null);
		
		//nasdaq extension
		StringField comboMatchID = new StringField(20034);
		node.put("ComboMatchID",  message.isSetField(comboMatchID) ? message.getField(comboMatchID).getValue() : null);
		
		PreviouslyReported previouslyReported = new PreviouslyReported();
		node.put("PreviouslyReported",  message.isSetField(previouslyReported) ? message.getField(previouslyReported).getValue() : null);
		
		putInstrument(node, message);
		
		LastQty lastQty = new LastQty();
		node.put("LastQty",  message.isSetField(lastQty) ? message.getField(lastQty).getValue() : null);
		
		LastPx lastPx = new LastPx();
		node.put("LastPx",  message.isSetField(lastPx) ? message.getField(lastPx).getValue() : null);
		
		SettlPrice settlPrice = new SettlPrice();
		node.put("SettlementPrice",  message.isSetField(settlPrice) ? message.getField(settlPrice).getValue() : null);

		SettlCurrAmt settlCurrAmt = new SettlCurrAmt();
	    node.put("SettlCurrAmt",  message.isSetField(settlCurrAmt) ? message.getField(settlCurrAmt).getValue() : null);

	    TradeDate tradeDate = new TradeDate();
	    node.put("TradeDate",  message.isSetField(tradeDate) ? message.getField(tradeDate).getValue() : null);

	    putTransactTime(node, message);
	    
	    SettlDate settlDate = new SettlDate();
	    node.put("SettlDate",  message.isSetField(settlDate) ? message.getField(settlDate).getValue() : null);
	    
	    // no sides
	    Side side = new Side();
	    Account account = new Account();
	    OrderID orderID = new OrderID();
	    AllocID allocID = new AllocID();
	    NoPartyIDs noPartyIDs = new NoPartyIDs();
	    quickfix.fix50sp2.TradeCaptureReport.NoSides noSidesGroup = new quickfix.fix50sp2.TradeCaptureReport.NoSides();
		List groupList = new ArrayList<Object>();  
		int groupCount = message.getGroupCount(NoSides.FIELD);
		for (int i=0; i<groupCount; i++) {
			Map<String, Object> noSidesNode = new LinkedHashMap<String, Object>();
			message.getGroup(i + 1, noSidesGroup);
			noSidesNode.put("Side", noSidesGroup.isSet(side) ? noSidesGroup.get(side).getValue() : null);
			noSidesNode.put("Account",  noSidesGroup.isSet(account) ? noSidesGroup.get(account).getValue() : null);
			noSidesNode.put("OrderID",  noSidesGroup.isSet(orderID) ? noSidesGroup.get(orderID).getValue() : null);
			noSidesNode.put("AllocID",  noSidesGroup.isSet(allocID) ? noSidesGroup.get(allocID).getValue() : null);
			
			groupList.add(noSidesNode);
		}
		node.put("NoSides", groupList);
		
		ClearingBusinessDate clearingBusinessDate = new ClearingBusinessDate();
	    node.put("ClearingBusinessDate",  message.isSetField(clearingBusinessDate) ? message.getField(clearingBusinessDate).getValue() : null);

	    SecondaryTrdType secondaryTrdType = new SecondaryTrdType();
	    node.put("SecondaryTrdType",  message.isSetField(secondaryTrdType) ? message.getField(secondaryTrdType).getValue() : null);
	    
	    CopyMsgIndicator copyMsgIndicator = new CopyMsgIndicator();
		node.put("CopyMsgIndicator",  message.isSetField(copyMsgIndicator) ? message.getField(copyMsgIndicator).getValue() : null);
		
		// put some log
		LocalDateTime now = LocalDateTime.now();
		node.put("MessageDropTime", now.toString());
		
		MsgSeqNum messageSeqNum = new MsgSeqNum();
		int messageSeqNumber = message.getHeader().getField(messageSeqNum).getValue();
		node.put("Id", messageSeqNumber + ":"+ trdID);
		
		return mapper.writeValueAsString(node);
	}
	
	public void putInstrument(Map<String, Object>  node, Message message) throws Exception{
		Symbol symbol = new Symbol();
		SecurityID securityID = new SecurityID();
		SecurityIDSource securityIDSource = new SecurityIDSource();
		node.put("Symbol",  message.isSetField(symbol) ? message.getField(symbol).getValue() : null);
		node.put("SecurityID",  message.isSetField(securityID) ? message.getField(securityID).getValue() : null);
		node.put("SecurityIDSource",  message.isSetField(securityIDSource) ? message.getField(securityIDSource).getValue() : null);
	}
	
	public void putTransactTime(Map<String, Object>  node, Message message) throws Exception{
		TransactTime transactTime = new TransactTime();
		LocalDateTime trxDate = message.getField(transactTime).getValue();
		LocalDateTime localizedDate = trxDate.plusHours(timeZoneOffset);
		node.put("TransactTime", localizedDate.format(basicFormat));
		node.put("TransactDate", localizedDate.format(basicDayFormat));
	}
}
