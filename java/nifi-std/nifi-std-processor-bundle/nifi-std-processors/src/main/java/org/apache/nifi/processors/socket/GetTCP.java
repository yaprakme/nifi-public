package org.apache.nifi.processors.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;



/**
 * 
 */
@TriggerSerially
@Tags({"socket", "tcp", "stream"})
@CapabilityDescription("tcp listener")
public class GetTCP extends AbstractProcessor {

    
    private final AtomicBoolean doStop = new AtomicBoolean(false);
    private SocketChannel channel = null;
    private ByteBuffer buffer = null;
    int maxFlowBatchSize;
    int demarcator;
	
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
    

    public static final PropertyDescriptor SOCKET_CONNECT_HOST = new PropertyDescriptor
            .Builder().name("Socket Connect Host")
            .description("Socket Connect Host")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor SOCKET_CONNECT_PORT = new PropertyDescriptor
            .Builder().name("Socket Connect Port")
            .description("Socket Connect Port")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor LOGON_PHRASE = new PropertyDescriptor
            .Builder().name("Logon Phrase")
            .description("A phrase to login in this tcp server.escape \n with \\n")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor DEMARCATOR = new PropertyDescriptor
            .Builder().name("End of message delimiter byte")
            .description("split the message by byte code . 10 (new line) is default")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("10")
            .build();
    
    public static final PropertyDescriptor MAX_FLOW_OUTPUT = new PropertyDescriptor
            .Builder().name("Max Flow Output")
            .description("Maximum Flow File count for each time")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
   
    public static final PropertyDescriptor MAX_SOCKET_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Socket Buffer")
            .description("The maximum size of the socket buffer that should be used.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();
   
    
    private Set<Relationship> relationships;

    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void setup(ProcessContext context) {     
    	maxFlowBatchSize = context.getProperty(MAX_FLOW_OUTPUT).asInteger();
    	demarcator = context.getProperty(DEMARCATOR).asInteger();
    	
    }
    
    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
    	 getLogger().debug("@OnUnscheduled");
    	 doStop.set(true);
    }
    
    @OnStopped
    public void stopped(final ProcessContext context) {
    	 getLogger().debug("@OnStopped");
    }
    
    public void stop() {
    	try {
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
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
        descriptors.add(LOGON_PHRASE);
        descriptors.add(MAX_FLOW_OUTPUT);
        descriptors.add(MAX_SOCKET_BUFFER_SIZE);
        descriptors.add(DEMARCATOR);
       
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
	@Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	final ComponentLog logger = getLogger();
    	String logonPhrase = context.getProperty(LOGON_PHRASE).getValue();
    	String socketConnectHost = context.getProperty(SOCKET_CONNECT_HOST).getValue();
        int socketConnectPort = context.getProperty(SOCKET_CONNECT_PORT).asInteger();
        int bufferSize = context.getProperty(MAX_SOCKET_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final StringBuilder sbFlow = new StringBuilder();	
        
		try {
		
			channel = SocketChannel.open();
    		channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
			SocketAddress socketAddr = new InetSocketAddress(socketConnectHost, socketConnectPort);
			channel.connect(socketAddr);
			buffer = ByteBuffer.allocate(bufferSize);
    	    doStop.set(false);
			
    	    logger.debug("Sending logonPhrase:"+logonPhrase);
    	    writeToChannel(logonPhrase+"\r\n");
			
			logger.debug("begin read");
			
			StringBuilder sbRow = new StringBuilder();	
			int index = 0;
			buffer.flip();
			
			while ( ! doStop.get()) {
				int byteCount = channel.read(buffer);
				if (byteCount < 0) {
					logger.error("end of stream");
				    break;
				}
				
				if(byteCount > 0) {
					
				   buffer.flip();
				   while (buffer.hasRemaining()) {
				        byte b = buffer.get();
				        if(!(b==10 ^ b==13 ^ b==-1)) {
				        	sbRow.append((char) b);
				        }
				        if (b == demarcator) {
				           String data = sbRow.toString().trim();
				           logger.debug(data);
				           sbRow.delete(0, sbRow.length());
				           index ++;	
				           sbFlow.append(data);
				           sbFlow.append((char) b);
				           if (index >= maxFlowBatchSize) {
				        	  createFlow(sbFlow.toString(), session);
				          	  session.commit();
				          	  sbFlow.delete(0, sbFlow.length());
				          	  index = 0;
				           }
				        }
				   }   
				}	
				buffer.clear();	
		    }
			
		} catch (Exception e) {
			logger.error("onTrigger : ", e);
		} finally {
			stop();
		}
		
		// Transfer any remaining files to SUCCESS
		if ( ! sbFlow.toString().equals("")) {
		   createFlow(sbFlow.toString(), session);
		}		
    }
	
	
	public void writeToChannel(String data) throws Exception {
	   channel.write(ByteBuffer.wrap(data.getBytes()));
	}
		
	
	public void createFlow(String data, ProcessSession session) {
    	FlowFile resultSetFF = session.create();
        resultSetFF = session.write(resultSetFF, out -> {
        	out.write(data.getBytes());
        });
        session.transfer(resultSetFF, REL_SUCCESS);  
	}
	
}