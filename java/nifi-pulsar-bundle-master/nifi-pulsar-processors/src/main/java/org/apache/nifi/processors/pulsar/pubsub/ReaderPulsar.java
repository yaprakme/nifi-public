/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.    See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar.pubsub;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;


@TriggerSerially
@Tags({"Pulsar", "Reader", "Ingest", "Ingress", "Topic", "PubSub"})
@CapabilityDescription("Uses pulsar reader interface so that client can manually position itself within a topic and reading all messages from a specified message onward.")
@Stateful(description = "persists last pulsar message id successfully published", scopes = { Scope.LOCAL })
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ReaderPulsar extends AbstractProcessor {

   

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was consumed from Pulsar.")
            .build();
    
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to Pulsar will be routed to this Relationship")
            .build();

    public static final PropertyDescriptor PULSAR_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("PULSAR_CLIENT_SERVICE")
            .displayName("Pulsar Client Service")
            .description("Specified the Pulsar Client Service that can be used to create Pulsar connections")
            .required(true)
            .identifiesControllerService(PulsarClientService.class)
            .build();

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("TOPIC")
            .displayName("Topic Name")
            .description("Specify the topic")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

   
    public static final PropertyDescriptor INITIAL_MESSAGE_ID = new PropertyDescriptor.Builder()
            .name("INITIAL_MESSAGE_ID")
            .displayName("Initial Message Id")
            .description("Specify the offset message id(exclusive) to start")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESULTSET_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("RESULTSET_MAX_MESSAGES")
            .displayName("Resultset Max Messages")
            .description("Set the maximum number of messages returned in result set")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("READ_TIMEOUT")
            .displayName("Read Timeout")
            .description("Maximum wait time on reader. In order to return result set either this amount of time has been elapsed or RESULTSET_MAX_MESSAGES has been reached")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .required(false)
            .build();
    

    protected static final List<PropertyDescriptor> PROPERTIES;
    protected static final Set<Relationship> RELATIONSHIPS;
    protected static final String PULSAR_MESSAGE_ID_ATTR = "pulsar-message-id";
    
   

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPIC);
        properties.add(INITIAL_MESSAGE_ID);
        properties.add(RESULTSET_MAX_MESSAGES);
        properties.add(READ_TIMEOUT);

        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    private PulsarClientService pulsarClientService;
  

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
    
        return results;
    }

    @OnScheduled
    public void init(ProcessContext context) {
        setPulsarClientService(context.getProperty(PULSAR_CLIENT_SERVICE).asControllerService(PulsarClientService.class));
    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
       
    }

    @OnStopped
    public void cleanUp(final ProcessContext context) {
        shutDown(context);
    }

    
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    	 final int resultSetSize = context.getProperty(RESULTSET_MAX_MESSAGES).evaluateAttributeExpressions().asInteger();
    	 final int readTimeout = context.getProperty(READ_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
    	 final StateManager stateManager = context.getStateManager();
    	 
    	 String initialMessageId = context.getProperty(INITIAL_MESSAGE_ID).evaluateAttributeExpressions().getValue();
    	 
 		 StateMap stateMap = null;
 		 MessageId messageId = null;

    	 try {
    		stateMap = context.getStateManager().getState(Scope.LOCAL);
    		
            if (stateMap.get(PULSAR_MESSAGE_ID_ATTR) != null) {
            	byte[] msgIdBytes = Base64.getDecoder().decode(stateMap.get(PULSAR_MESSAGE_ID_ATTR));
        		messageId = MessageId.fromByteArray(msgIdBytes);
            }else {
            	if (initialMessageId != null) {
            		byte[] msgIdBytes = Base64.getDecoder().decode(initialMessageId);
            		messageId = MessageId.fromByteArray(msgIdBytes);
            	}else {
            		messageId = MessageId.earliest;
            	}
            }
        
            List<FlowFile> resultSetFlowFiles = new ArrayList<>();
        	Reader reader = getPulsarReader(context, messageId);
        	String lastMessageId = null;
        	for (int i = 0; i < resultSetSize; i++) {
        		if (reader.hasMessageAvailable()) {
        		   Message<byte[]> message = reader.readNext(readTimeout, TimeUnit.SECONDS);	
        		   if (message != null) {
        			   
        			   FlowFile ff = session.create();		 
                       session.write(ff, new OutputStreamCallback() {
	           				@Override public void process(OutputStream out) throws IOException {
	           					out.write(message.getData());
	           				}
           			   });
                       
                       if (getLogger().isDebugEnabled()) {
                    	   getLogger().debug(new String(message.getData(), StandardCharsets.UTF_8));
                       }
                       
                       lastMessageId = Base64.getEncoder().encodeToString(message.getMessageId().toByteArray());
                       ff = session.putAttribute(ff, "pulsar-message-id", lastMessageId);
                       resultSetFlowFiles.add(ff);
        		   }else {  
        			   break;
        		   }
        		}  
			}
        	
        	getLogger().debug("lastMessageId="+lastMessageId);
        	
        	if (lastMessageId != null) {
        		final Map<String, String> newState = new HashMap<>();
             	newState.put(PULSAR_MESSAGE_ID_ATTR,  lastMessageId);
        		stateManager.setState(newState, Scope.LOCAL);
        		session.transfer(resultSetFlowFiles, REL_SUCCESS);
        	}else {
        		session.rollback();
        	}

        } catch (Exception e) {
            getLogger().error("Unable to read ", e);
            context.yield();
        }
    }
    
    protected synchronized Reader getPulsarReader(ProcessContext context, MessageId messageId) throws PulsarClientException {
        Reader reader = (Reader) getPulsarClientService().getPulsarClient().newReader()
                .topic(context.getProperty(TOPIC).evaluateAttributeExpressions().getValue())
        		.startMessageId(messageId)
        		.create();
        return reader;
    }

    protected synchronized PulsarClientService getPulsarClientService() {
       return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
       this.pulsarClientService = pulsarClientService;
    }

  
}
