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
package org.apache.nifi.processors.pulsar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.cache.PulsarClientLRUCache;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public abstract class AbstractPulsarConsumerProcessor<T> extends AbstractProcessor {

    static final AllowableValue EXCLUSIVE = new AllowableValue("Exclusive", "Exclusive", "There can be only 1 consumer on the same topic with the same subscription name");
    static final AllowableValue SHARED = new AllowableValue("Shared", "Shared", "Multiple consumer will be able to use the same subscription name and the messages");
    static final AllowableValue FAILOVER = new AllowableValue("Failover", "Failover", "Multiple consumer will be able to use the same subscription name but only 1 consumer "
            + "will receive the messages. If that consumer disconnects, one of the other connected consumers will start receiving messages.");

   
    public volatile boolean keepRunning = true;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was consumed from Pulsar.")
            .build();

    public static final PropertyDescriptor PULSAR_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("PULSAR_CLIENT_SERVICE")
            .displayName("Pulsar Client Service")
            .description("Specified the Pulsar Client Service that can be used to create Pulsar connections")
            .required(true)
            .identifiesControllerService(PulsarClientService.class)
            .build();

    public static final PropertyDescriptor TOPICS = new PropertyDescriptor.Builder()
            .name("TOPICS")
            .displayName("Topic Names")
            .description("Specify the topics this consumer will subscribe on. "
                    + "You can specify multiple topics in a comma-separated list."
                    + "E.g topicA, topicB, topicC ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOPICS_PATTERN = new PropertyDescriptor.Builder()
            .name("TOPICS_PATTERN")
            .displayName("Topics Pattern")
            .description("Alternatively, you can specify a pattern for topics that this consumer "
                    + "will subscribe on. It accepts a regular expression and will be compiled into "
                    + "a pattern internally. E.g. \"persistent://my-tenant/ns-abc/pattern-topic-.*\" "
                    + "would subscribe to any topic whose name started with 'pattern-topic-' that was in "
                    + "the 'ns-abc' namespace, and belonged to the 'my-tenant' tentant.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_NAME = new PropertyDescriptor.Builder()
            .name("SUBSCRIPTION_NAME")
            .displayName("Subscription Name")
            .description("Specify the subscription name for this consumer.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ACK_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ACK_TIMEOUT")
            .displayName("Acknowledgment Timeout")
            .description("Set the timeout for unacked messages. Messages that are not acknowledged within the "
                    + "configured timeout will be replayed. This value needs to be greater than 10 seconds.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .required(false)
            .build();

  
    
    public static final PropertyDescriptor CONSUMER_NAME = new PropertyDescriptor.Builder()
            .name("CONSUMER_NAME")
            .displayName("Consumer Name")
            .description("Set the name of the consumer to uniquely identify this client on the Broker")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRIORITY_LEVEL = new PropertyDescriptor.Builder()
            .name("PRIORITY_LEVEL")
            .displayName("Consumer Priority Level")
            .description("Sets priority level for the shared subscription consumers to which broker "
                    + "gives more priority while dispatching messages. Here, broker follows descending "
                    + "priorities. (eg: 0=max-priority, 1, 2,..) ")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor RECEIVER_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("RECEIVER_QUEUE_SIZE")
            .displayName("Consumer Receiver Queue Size")
            .description("The consumer receive queue controls how many messages can be accumulated "
                    + "by the Consumer before the application calls Consumer.receive(). Using a higher "
                    + "value could potentially increase the consumer throughput at the expense of bigger "
                    + "memory utilization. \n"
                    + "Setting the consumer queue size as zero, \n"
                    + "\t - Decreases the throughput of the consumer, by disabling pre-fetching of messages. \n"
                    + "\t - Doesn't support Batch-Message: if consumer receives any batch-message then it closes consumer "
                    + "connection with broker and consumer will not be able receive any further message unless batch-message "
                    + "in pipeline is removed")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();

    public static final PropertyDescriptor SUBSCRIPTION_TYPE = new PropertyDescriptor.Builder()
            .name("SUBSCRIPTION_TYPE")
            .displayName("Subscription Type")
            .description("Select the subscription type to be used when subscribing to the topic.")
            .required(true)
            .allowableValues(EXCLUSIVE, SHARED, FAILOVER)
            .defaultValue(SHARED.getValue())
            .build();
    
    public static final PropertyDescriptor BATCHING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("BATCHING_MAX_MESSAGES")
            .displayName("Batching Max Messages")
            .description("Set the maximum number of messages permitted in a batch.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor BATCHING_MAX_BYTES = new PropertyDescriptor.Builder()
            .name("BATCHING_MAX_BYTES")
            .displayName("Batching Max Bytes")
            .description("Set the maximum number of bytes permitted in a batch. default = 10 * 1024 * 1024")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10485760")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor BATCH_TIMEOUT = new PropertyDescriptor.Builder()
            .name("BATCH_TIMEOUT")
            .displayName("Batch Timeout")
            .description("Set the maximum time permitted to accumulate a batch ")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("200 ms")
            .build();
    
    protected static final List<PropertyDescriptor> PROPERTIES;
    protected static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPICS);
        properties.add(TOPICS_PATTERN);
        properties.add(SUBSCRIPTION_NAME);
        properties.add(CONSUMER_NAME);
        properties.add(ACK_TIMEOUT);
        properties.add(PRIORITY_LEVEL);
        properties.add(RECEIVER_QUEUE_SIZE);
        properties.add(SUBSCRIPTION_TYPE);
        properties.add(BATCHING_MAX_MESSAGES);
        properties.add(BATCH_TIMEOUT);
        properties.add(BATCHING_MAX_BYTES);

        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    private PulsarClientService pulsarClientService;
    private PulsarClientLRUCache<String, Consumer<T>> consumers;
    private ExecutorService consumerPool;
    private ExecutorCompletionService<List<Message<T>>> consumerService;
    private ExecutorService ackPool;
    private ExecutorCompletionService<Object> ackService;

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
        boolean topicsSet = validationContext.getProperty(TOPICS).isSet();
        boolean topicPatternSet = validationContext.getProperty(TOPICS_PATTERN).isSet();

        if (!topicsSet && !topicPatternSet) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "At least one of the 'Topics' or 'Topic Pattern' properties must be specified.").build());
        } else if (topicsSet && topicPatternSet) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "Only one of the two properties ('Topics' and 'Topic Pattern') can be specified.").build());
        }

        if (validationContext.getProperty(ACK_TIMEOUT).asTimePeriod(TimeUnit.SECONDS) < 10) {
           results.add(new ValidationResult.Builder().valid(false).explanation(
               "Acknowledgment Timeout needs to be greater than 10 seconds.").build());
        }

        return results;
    }

    @OnScheduled
    public void init(ProcessContext context) {
        setPulsarClientService(context.getProperty(PULSAR_CLIENT_SERVICE).asControllerService(PulsarClientService.class));
        keepRunning = true;
    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
    	 keepRunning = false;
    	 getLogger().debug("@OnUnscheduled");
    }

    @OnStopped
    public void cleanUp(final ProcessContext context) {
        getConsumers().clear();
        getLogger().debug("@OnStopped");
    }

    /**
     * Method returns a string that uniquely identifies a consumer by concatenating
     * the topic name and subscription properties together.
     */
    protected String getConsumerId(final ProcessContext context, FlowFile flowFile) {
        if (context == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer();

        if (context.getProperty(TOPICS).isSet()) {
           sb.append(context.getProperty(TOPICS).evaluateAttributeExpressions(flowFile).getValue());
        } else {
           sb.append(context.getProperty(TOPICS_PATTERN).getValue());
        }

        sb.append("-").append(context.getProperty(SUBSCRIPTION_NAME).getValue());

        if (context.getProperty(CONSUMER_NAME).isSet()) {
            sb.append("-").append(context.getProperty(CONSUMER_NAME).getValue());
        }
        return sb.toString();
    }

   
    protected synchronized Consumer<T> getConsumer(ProcessContext context, String topic) throws PulsarClientException {

        /* Avoid creating producers for non-existent topics */
    	if (topic == null || topic.equals("")) {
          return null;
        }

        Consumer<T> consumer = getConsumers().get(topic);

        if (consumer != null && consumer.isConnected()) {
           return consumer;
        }

        // Create a new consumer and validate that it is connected before returning it.
        consumer = getConsumerBuilder(context).subscribe();
        if (consumer != null && consumer.isConnected()) {
           getConsumers().put(topic, consumer);
        }

        return (consumer != null && consumer.isConnected()) ? consumer : null;
    }

    protected synchronized ConsumerBuilder<T> getConsumerBuilder(ProcessContext context) throws PulsarClientException {

        ConsumerBuilder<T> builder = (ConsumerBuilder<T>) getPulsarClientService().getPulsarClient().newConsumer();

        if (context.getProperty(TOPICS).isSet()) {
            builder = builder.topic(Arrays.stream(context.getProperty(TOPICS).evaluateAttributeExpressions().getValue().split("[, ]"))
                    .map(String::trim).toArray(String[]::new));
        } else if (context.getProperty(TOPICS_PATTERN).isSet()) {
            builder = builder.topicsPattern(context.getProperty(TOPICS_PATTERN).getValue());
        }

        if (context.getProperty(CONSUMER_NAME).isSet()) {
            builder = builder.consumerName(context.getProperty(CONSUMER_NAME).getValue());
        }

        return builder.subscriptionName(context.getProperty(SUBSCRIPTION_NAME).getValue())
                .ackTimeout(context.getProperty(ACK_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS)
                .priorityLevel(context.getProperty(PRIORITY_LEVEL).asInteger())
                .receiverQueueSize(context.getProperty(RECEIVER_QUEUE_SIZE).asInteger())
                .subscriptionType(SubscriptionType.valueOf(context.getProperty(SUBSCRIPTION_TYPE).getValue()))
                .batchReceivePolicy(BatchReceivePolicy.builder()
		             .maxNumMessages(context.getProperty(BATCHING_MAX_MESSAGES).evaluateAttributeExpressions().asInteger())
		             .maxNumBytes(context.getProperty(BATCHING_MAX_BYTES).evaluateAttributeExpressions().asInteger())
		             .timeout(context.getProperty(BATCH_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS)
		             .build())
                ;
    }

    protected synchronized ExecutorService getConsumerPool() {
        return consumerPool;
    }

    protected synchronized void setConsumerPool(ExecutorService pool) {
        this.consumerPool = pool;
    }

    protected synchronized ExecutorCompletionService<List<Message<T>>> getConsumerService() {
        return consumerService;
    }

    protected synchronized void setConsumerService(ExecutorCompletionService<List<Message<T>>> service) {
        this.consumerService = service;
    }

    protected synchronized ExecutorService getAckPool() {
       return ackPool;
    }

    protected synchronized void setAckPool(ExecutorService pool) {
       this.ackPool = pool;
    }

    protected synchronized ExecutorCompletionService<Object> getAckService() {
       return ackService;
    }

    protected synchronized void setAckService(ExecutorCompletionService<Object> ackService) {
       this.ackService = ackService;
    }

    protected synchronized PulsarClientService getPulsarClientService() {
       return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
       this.pulsarClientService = pulsarClientService;
    }

    protected synchronized PulsarClientLRUCache<String, Consumer<T>> getConsumers() {
        if (consumers == null) {
           consumers = new PulsarClientLRUCache<String, Consumer<T>>(20);
        }
        return consumers;
    }

    protected void setConsumers(PulsarClientLRUCache<String, Consumer<T>> consumers) {
        this.consumers = consumers;
    }
}
