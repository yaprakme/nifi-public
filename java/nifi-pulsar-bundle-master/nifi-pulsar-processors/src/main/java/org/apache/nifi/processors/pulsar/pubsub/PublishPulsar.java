
package org.apache.nifi.processors.pulsar.pubsub;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;

@SeeAlso({ConsumePulsar.class})
@Tags({"Apache", "Pulsar", "Put", "Send", "Message", "PubSub"})
@CapabilityDescription("Sends the contents of a FlowFile as a message to Apache Pulsar using the Pulsar Producer API The complementary NiFi processor for fetching messages is ConsumePulsar.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PublishPulsar extends AbstractPulsarProducerProcessor<byte[]> {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String topic = context.getProperty(TOPIC).evaluateAttributeExpressions(flowFile).getValue();
        final Producer<byte[]> producer = getProducer(context, topic);

        /* If we are unable to create a producer, then we know we won't be able
         * to send the message successfully, so go ahead and route to failure now.
         */
        if (producer == null) {
            getLogger().error("Unable to publish to topic {}", new Object[] {topic});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            send(producer, session, flowFile);
        } catch (final Exception e) {
            getLogger().error("Failed to send to Pulsar Server due to {}", new Object[]{e});
            throw new ProcessException(e);
        }
      
    }

    /**
     * Sends the FlowFile content
     */
    private void send(Producer<byte[]> producer, ProcessSession session, FlowFile flowFile) throws Exception {
      
        InputStream in = session.read(flowFile);
        byte[] messageContent = IOUtils.toByteArray(in);
        IOUtils.closeQuietly(in);
        
        MessageId messageId = null;
        
        try {
			messageId = producer.send(messageContent);
			session.transfer(flowFile, REL_SUCCESS);
		} catch (Exception e) {
			 getLogger().error("Failed to send to Pulsar Server due to {}", new Object[]{e});
		} 
        
        if (messageId == null) {
	       	 // send to failure
       	     session.transfer(flowFile, REL_FAILURE);  
        }
      
    }
}
