/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pulsar.pubsub;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;

@SeeAlso({PublishPulsar.class})
@Tags({"Pulsar", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Consume"})
@CapabilityDescription("Consumes messages from Apache Pulsar. The complementary NiFi processor for sending messages is PublishPulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class ConsumePulsar extends AbstractPulsarConsumerProcessor<byte[]> {

	
	  
	 private static final List<PropertyDescriptor> propertyDescriptors;
	 
	 static {
		 final List<PropertyDescriptor> properties = new ArrayList<>(PROPERTIES);
		 propertyDescriptors = Collections.unmodifiableList(properties);
	 }
	 
	 @Override
     public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
     }
	        
	

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
        	
            Consumer<byte[]> consumer = getConsumer(context, getConsumerId(context, session.get()));

            if (consumer == null) {
                context.yield();
                return;
            }
            
            consumeBatch(consumer, context, session);
           
        } catch (PulsarClientException e) {
            getLogger().error("Unable to consume from Pulsar Topic ", e);
            context.yield();
            throw new ProcessException(e);
        }
    }

   
    private void consumeBatch(Consumer<byte[]> consumer, ProcessContext context, ProcessSession session) throws PulsarClientException {
        try {
        	
        	Messages<byte[]>  messages = consumer.batchReceive();
        	
        	for (Message<byte[]> msg : messages) {
            		
	            // Skip empty messages, as they cause NPE's when we write them to the OutputStream
	            if (msg.getValue() == null || msg.getValue().length < 1) {
	              continue;
	            }
	            
	            if (getLogger().isDebugEnabled()) {
	               getLogger().debug("Message Id="+Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray())+" Message="+new String(msg.getData(), StandardCharsets.UTF_8));
	            }
	            
	            final byte[] messageContent = msg.getValue();
	            FlowFile ff = session.create();
	            ff = session.write(ff, out -> {
	            	out.write(messageContent);
	            });
	            ff = session.putAttribute(ff, "pulsar-message-id", Base64.getEncoder().encodeToString(msg.getMessageId().toByteArray()));
	            session.transfer(ff, REL_SUCCESS);
                    
        	}  
        	
            consumer.acknowledge(messages);
            session.commit();    
           
        } catch (PulsarClientException e) {
            context.yield();
            session.rollback();
        }
    }
   
}