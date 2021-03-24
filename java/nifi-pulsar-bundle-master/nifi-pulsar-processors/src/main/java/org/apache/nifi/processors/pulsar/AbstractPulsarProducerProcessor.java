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
package org.apache.nifi.processors.pulsar;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.cache.PulsarClientLRUCache;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;


public abstract class AbstractPulsarProducerProcessor<T> extends AbstractProcessor {


    static final AllowableValue COMPRESSION_TYPE_NONE = new AllowableValue("NONE", "None", "No compression");
    static final AllowableValue COMPRESSION_TYPE_LZ4 = new AllowableValue("LZ4", "LZ4", "Compress with LZ4 algorithm.");
    static final AllowableValue COMPRESSION_TYPE_ZLIB = new AllowableValue("ZLIB", "ZLIB", "Compress with ZLib algorithm");

    static final AllowableValue MESSAGE_ROUTING_MODE_CUSTOM_PARTITION = new AllowableValue("CustomPartition", "Custom Partition", "Route messages to a custom partition");
    static final AllowableValue MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION = new AllowableValue("RoundRobinPartition", "Round Robin Partition", "Route messages to all "
                                                                                                                       + "partitions in a round robin manner");
    static final AllowableValue MESSAGE_ROUTING_MODE_SINGLE_PARTITION = new AllowableValue("SinglePartition", "Single Partition", "Route messages to a single partition");

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for which all content was sent to Pulsar.")
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
            .description("The name of the Pulsar Topic.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    
    public static final PropertyDescriptor BATCHING_ENABLED = new PropertyDescriptor.Builder()
            .name("BATCHING_ENABLED")
            .displayName("Batching Enabled")
            .description("Control whether automatic batching of messages is enabled for the producer. ")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor BATCHING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("BATCHING_MAX_MESSAGES")
            .displayName("Batching Max Messages")
            .description("Set the maximum number of messages permitted in a batch within the Pulsar client. "
                    + "default: 100. If set to a value greater than 1, messages will be queued until this "
                    + "threshold is reached or the batch interval has elapsed, whichever happens first.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor BATCH_INTERVAL = new PropertyDescriptor.Builder()
            .name("BATCH_INTERVAL")
            .displayName("Batch Interval")
            .description("Set the time period within which the messages sent will be batched if batch messages are enabled."
                    + " If set to a non zero value, messages will be queued until this time interval has been reached OR"
                    + " until the Batching Max Messages threshould has been reached, whichever occurs first.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("200 ms")
            .build();
  
    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("COMPRESSION_TYPE")
            .displayName("Compression Type")
            .description("Set the compression type for the producer.")
            .required(true)
            .allowableValues(COMPRESSION_TYPE_NONE, COMPRESSION_TYPE_LZ4, COMPRESSION_TYPE_ZLIB)
            .defaultValue(COMPRESSION_TYPE_NONE.getValue())
            .build();

    public static final PropertyDescriptor MESSAGE_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("MESSAGE_DEMARCATOR")
            .displayName("Message Demarcator")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Specifies the string (interpreted as UTF-8) to use for demarcating multiple messages within "
                + "a single FlowFile. If not specified, the entire content of the FlowFile will be used as a single message. If specified, the "
                + "contents of the FlowFile will be split on this delimiter and each section sent as a separate Pulsar message. "
                + "To enter special character such as 'new line' use CTRL+Enter or Shift+Enter, depending on your OS.")
            .build();

    public static final PropertyDescriptor MESSAGE_ROUTING_MODE = new PropertyDescriptor.Builder()
            .name("MESSAGE_ROUTING_MODE")
            .displayName("Message Routing Mode")
            .description("Set the message routing mode for the producer. This applies only if the destination topic is partitioned")
            .required(true)
            .allowableValues(MESSAGE_ROUTING_MODE_CUSTOM_PARTITION, MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION, MESSAGE_ROUTING_MODE_SINGLE_PARTITION)
            .defaultValue(MESSAGE_ROUTING_MODE_ROUND_ROBIN_PARTITION.getValue())
            .build();

    public static final PropertyDescriptor PENDING_MAX_MESSAGES = new PropertyDescriptor.Builder()
            .name("PENDING_MAX_MESSAGES")
            .displayName("Max Pending Messages")
            .description("Set the max size of the queue holding the messages pending to receive an "
                    + "acknowledgment from the broker.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("1000")
            .build();

    protected static final List<PropertyDescriptor> PROPERTIES;
    protected static final Set<Relationship> RELATIONSHIPS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PULSAR_CLIENT_SERVICE);
        properties.add(TOPIC);
        //properties.add(ASYNC_ENABLED);
        //properties.add(MAX_ASYNC_REQUESTS);
        properties.add(BATCHING_ENABLED);
        properties.add(BATCHING_MAX_MESSAGES);
        properties.add(BATCH_INTERVAL);
        //properties.add(BLOCK_IF_QUEUE_FULL);
        properties.add(COMPRESSION_TYPE);
        properties.add(MESSAGE_ROUTING_MODE);
        //properties.add(MESSAGE_DEMARCATOR);
        properties.add(PENDING_MAX_MESSAGES);
        PROPERTIES = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private PulsarClientService pulsarClientService;
    private PulsarClientLRUCache<String, Producer<T>> producers;
    private ExecutorService publisherPool;

    // Used to sync between onTrigger method and shutdown code block.
    protected AtomicBoolean canPublish = new AtomicBoolean();

    // Used to track whether we are reporting errors back to the user or not.
    protected AtomicBoolean trackFailures = new AtomicBoolean();



    @OnScheduled
    public void init(ProcessContext context) {
        setPulsarClientService(context.getProperty(PULSAR_CLIENT_SERVICE).asControllerService(PulsarClientService.class));
    }

    @OnUnscheduled
    public void shutDown(final ProcessContext context) {
   
    }

    @OnStopped
    public void cleanUp(final ProcessContext context) {
      
    }

 


    protected synchronized Producer<T> getProducer(ProcessContext context, String topic) {

        /* Avoid creating producers for non-existent topics */
        if (topic == null || topic.equals("")) {
           return null;
        }

        Producer<T> producer = getProducers().get(topic);

        try {
            if (producer != null && producer.isConnected()) {
              return producer;
            }

            producer = getBuilder(context, topic).create();

            if (producer != null && producer.isConnected()) {
              getProducers().put(topic, producer);
            }
        } catch (PulsarClientException e) {
            getLogger().error("Unable to create Pulsar Producer ", e);
            producer = null;
        }
        return (producer != null && producer.isConnected()) ? producer : null;
    }

    private synchronized ProducerBuilder<T> getBuilder(ProcessContext context, String topic) {
        ProducerBuilder<T> builder = (ProducerBuilder<T>) getPulsarClientService().getPulsarClient().newProducer();
        return builder.topic(topic)
                      .enableBatching(context.getProperty(BATCHING_ENABLED).asBoolean())
                      .batchingMaxMessages(context.getProperty(BATCHING_MAX_MESSAGES).evaluateAttributeExpressions().asInteger())
                      .batchingMaxPublishDelay(context.getProperty(BATCH_INTERVAL).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS)
                      .compressionType(CompressionType.valueOf(context.getProperty(COMPRESSION_TYPE).getValue()))
                      .maxPendingMessages(context.getProperty(PENDING_MAX_MESSAGES).evaluateAttributeExpressions().asInteger())
                      .messageRoutingMode(MessageRoutingMode.valueOf(context.getProperty(MESSAGE_ROUTING_MODE).getValue()));
    }

    protected synchronized PulsarClientService getPulsarClientService() {
       return pulsarClientService;
    }

    protected synchronized void setPulsarClientService(PulsarClientService pulsarClientService) {
       this.pulsarClientService = pulsarClientService;
    }

    protected synchronized PulsarClientLRUCache<String, Producer<T>> getProducers() {
       if (producers == null) {
         producers = new PulsarClientLRUCache<String, Producer<T>>(20);
       }
       return producers;
    }

    protected synchronized void setProducers(PulsarClientLRUCache<String, Producer<T>> producers) {
       this.producers = producers;
    }

    protected synchronized ExecutorService getPublisherPool() {
       return publisherPool;
    }

    protected synchronized void setPublisherPool(ExecutorService publisherPool) {
       this.publisherPool = publisherPool;
    }

   
}