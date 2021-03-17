package org.apache.nifi.processors.fix;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.fix.handler.DefaultApplication;
import org.apache.nifi.processors.fix.handler.DefaultMessageHandler;
import org.apache.nifi.processors.fix.handler.MessageHandlerImpl;
import org.apache.nifi.processors.fix.store.NifiStateManagerStore;

import quickfix.DefaultMessageFactory;
import quickfix.Dictionary;
import quickfix.Initiator;
import quickfix.LogFactory;
import quickfix.Message;
import quickfix.MessageFactory;
import quickfix.MessageStoreFactory;
import quickfix.SLF4JLogFactory;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;
import quickfix.field.BeginString;
import quickfix.field.SenderCompID;
import quickfix.field.SenderSubID;
import quickfix.field.TargetCompID;
import quickfix.field.TargetSubID;



/**
 * 
 */
@Tags({"fix", "quickfix", "ingest", "nasdaq"})
@CapabilityDescription("Quickfix initiator implementation")
@Stateful(description = "", scopes = { Scope.CLUSTER })
public class QuickFixInitiator extends AbstractProcessor {

    private MessageHandlerImpl messageHandler = null;
    
    private LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private Initiator initiator = null;
    private final AtomicBoolean doStop = new AtomicBoolean(false);
    
	
	public static final String INITIATOR = "initiator";
	public static final String ACCEPTOR = "acceptor";


	
    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully handled are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "Files that could not be handled for some reason are transferred to this relationship")
            .build();
    
    public static final PropertyDescriptor START_TIME = new PropertyDescriptor
            .Builder().name("Start Time")
            .description("Start Time")
            .defaultValue("00:00:00 Europe/Istanbul")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor END_TIME = new PropertyDescriptor
            .Builder().name("End Time")
            .description("End Time")
            .defaultValue("00:00:00 Europe/Istanbul")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor SOCKET_CONNECT_HOST = new PropertyDescriptor
            .Builder().name("Socket Connect Host")
            .description("Socket Connect Host")
            //.defaultValue("localhost")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor SOCKET_CONNECT_PORT = new PropertyDescriptor
            .Builder().name("Socket Connect Port")
            .description("Socket Connect Port")
            //.defaultValue("9882")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor SENDER_COMPONENT_ID = new PropertyDescriptor
            .Builder().name("Sender Component ID")
            .description("Sender Component ID")
            //.defaultValue("NIFI")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor SENDER_SUB_ID = new PropertyDescriptor
            .Builder().name("Sender Sub ID")
            .description("Sender Sub ID")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
   
    public static final PropertyDescriptor TARGET_COMPONENT_ID = new PropertyDescriptor
            .Builder().name("Target Component ID")
            .description("Target Component ID")
            //.defaultValue("BANZAI")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor BEGIN_STRING = new PropertyDescriptor
            .Builder().name("Begin String")
            .description("Begin String is fix version such as FIXT.1.1 FIX.4.4")
            .defaultValue("FIXT.1.1")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
   
    
    public static final PropertyDescriptor DEFAULT_APPL_VER_ID = new PropertyDescriptor
            .Builder().name("Default Application Verify ID")
            .description("Required only for FIXT 1.1 (and newer). Ignored for earlier transport versions. options are FIX.5.0SP2 FIX.5.0SP1 FIX.5.0")
            //.defaultValue("FIX.5.0SP2")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
   
   
    public static final PropertyDescriptor RESET_SEQUENCE_NUMBER = new PropertyDescriptor
            .Builder().name("Reset Sequence Number")
            .description("Enter Y for reset")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    private Set<Relationship> relationships;

    
    private DBCPService passwordService; 
    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    

    @OnScheduled
    public void setup(ProcessContext context) {
    	final ComponentLog logger = getLogger();
    	String beginString = context.getProperty(BEGIN_STRING).getValue();
        String targetCompId = context.getProperty(TARGET_COMPONENT_ID).getValue();
        String senderCompId = context.getProperty(SENDER_COMPONENT_ID).getValue();
        String senderSubId = context.getProperty(SENDER_SUB_ID).getValue();
        String defaultApplVerID = context.getProperty(DEFAULT_APPL_VER_ID).getValue();
        String socketConnectHost = context.getProperty(SOCKET_CONNECT_HOST).getValue();
        String socketConnectPort = context.getProperty(SOCKET_CONNECT_PORT).getValue();
        String username = context.getProperty(USERNAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();
        String resetSeq = context.getProperty(RESET_SEQUENCE_NUMBER).getValue();
        String startTime = context.getProperty(START_TIME).getValue();
        String endTime = context.getProperty(END_TIME).getValue();
        
        
        SessionID sessionID;
        
    	try {
    
    		SessionSettings settings = new SessionSettings(); 
    		sessionID =  new SessionID(new BeginString(beginString), new SenderCompID(senderCompId), new SenderSubID(senderSubId), new TargetCompID(targetCompId), new TargetSubID());
    		
    		Dictionary sessionDictionary = new Dictionary(); 
    		sessionDictionary.setString(quickfix.Session.SETTING_HEARTBTINT, "30");   
    		sessionDictionary.setLong("ReconnectInterval", 5);
    		sessionDictionary.setString("ConnectionType", "initiator");
    	    
    	    if (defaultApplVerID != null) {
    	    	sessionDictionary.setString("DefaultApplVerID", defaultApplVerID);
    	    }
    	    
    	    sessionDictionary.setString("SocketConnectHost", socketConnectHost);
            sessionDictionary.setString("SocketConnectPort", socketConnectPort);
 
    	    sessionDictionary.setString("StartTime", startTime);
    	    sessionDictionary.setString("EndTime", endTime);
    	    
    	    sessionDictionary.setString("PersistMessages", "N");
    	    sessionDictionary.setString("AllowUnknownMsgFields", "Y");
    	    sessionDictionary.setString("ValidateUserDefinedFields", "N");
    	    sessionDictionary.setString("ValidateIncomingMessage", "N");
    	    sessionDictionary.setString("CheckLatency", "N");
    	        	   
    	    // set dynamic properties
    	    for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
	            PropertyDescriptor pd = property.getKey();
	            if (pd.isDynamic()) {
	                if (property.getValue() != null) {
	                	sessionDictionary.setString(pd.getName(), property.getValue());
	                }
	            }
	        } 
    	    
    	    settings.set(sessionID, sessionDictionary);
 
    	    LogFactory logFactory =  new SLF4JLogFactory(settings);
    		MessageStoreFactory messageStoreFactory = new NifiStateManagerStore(context.getStateManager());
    		MessageFactory messageFactory = new DefaultMessageFactory();
    		quickfix.Application application = null;
    		
    		messageHandler = new DefaultMessageHandler();
    		application = new DefaultApplication(username, password, resetSeq, logger, queue);
    		
    		initiator = new SocketInitiator(application, messageStoreFactory, settings, logFactory,messageFactory);	
    	    initiator.start(); 
    	    doStop.set(false);
    	    
		} catch (Exception e) {
			logger.error("setup : ", e);
		}
		
    }
    
    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
    	 getLogger().debug("@OnUnscheduled");
    	 initiator.stop();	
    	 doStop.set(true);
    }
    

    private List<PropertyDescriptor> descriptors;
  
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
    
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(Validator.VALID)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SOCKET_CONNECT_HOST);
        descriptors.add(SOCKET_CONNECT_PORT);
        descriptors.add(SENDER_COMPONENT_ID);
        descriptors.add(SENDER_SUB_ID);
        descriptors.add(TARGET_COMPONENT_ID);
        descriptors.add(BEGIN_STRING);
        descriptors.add(DEFAULT_APPL_VER_ID);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(START_TIME);
        descriptors.add(END_TIME);
        descriptors.add(RESET_SEQUENCE_NUMBER);
   
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    
    
	@Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	final ComponentLog logger = getLogger();
    	
		try {
			
			while ( ! doStop.get()) {
				
				Message message =  null;
				while ((message = queue.poll()) != null) {
					
					FlowFile ff = null;
					try {
						String json = messageHandler.parse(message);
						if (json != null) {
							ff = session.create();
							logger.debug("toJson="+json);
							ff = session.write(ff, out -> {
			                	out.write(json.getBytes(StandardCharsets.UTF_8));
			                });
							session.transfer(ff, REL_SUCCESS);
						}
					} catch (Exception e) {
						logger.error("UnexpectedException : ", e);
						ff = session.create();
						ff = session.write(ff, out -> {
		                	out.write(e.getMessage().getBytes(StandardCharsets.UTF_8));
		                });
						session.putAttribute(ff, "original-message", message.toRawString());
						session.transfer(ff, REL_FAILURE);
					}
					session.commit();
				}
				
				Thread.currentThread().sleep(500);
			}
				
		        
		} catch (Exception e) {
			logger.error("onTrigger : ", e);
		}     
    }
 
}