package org.apache.nifi.processors.redis;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.redis.util.RedisAction;
import org.apache.nifi.redis.RedisConnectionPool;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;



/**
 * This processor provides TimeWindow component using the Redis in-memory data structure store
 */
@Tags({"window", "softtech", "redis", "cache", "time"})
@CapabilityDescription("Groups events by some fileds in a certain time window to aggregate on some other fields")
public class TimeWindow extends AbstractProcessor {

   
    // relationships
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original flow files are transferred to this relationship")
            .build();

    public static final Relationship REL_AGGREGATED = new Relationship.Builder()
            .name("aggregated")
            .description( "Aggregated flow files are transferred to this relationship")
            .build();
    
    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("fail")
            .description( "Failed flow files are transferred to this relationship")
            .build();

    // properties
    public static final String AGG_SUM = "sum";
    public static final String AGG_COUNT = "count";
    public static final String AGG_AVG = "avg";
    public static final AllowableValue AGG_SUM_AV = new AllowableValue(AGG_SUM, "Sum", "Sum");
    public static final AllowableValue AGG_AVG_AV = new AllowableValue(AGG_AVG, "Avg", "avg");
    public static final AllowableValue AGG_COUNT_AV = new AllowableValue(AGG_COUNT, "Count", "Count");
    public static final String DEFAULT_COUNT_KEY = "DEFAULT_COUNT_KEY";
    public static final String DEFAULT_COUNT_VALUE = "1";
    
    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .identifiesControllerService(RedisConnectionPool.class)
            .required(true)
            .build();
 
    
    public static final PropertyDescriptor MAX_BATCH_SIZE  = new PropertyDescriptor
            .Builder().name("max-batch-count")
            .displayName("Max Flow file Batch Count")
            .description("Maximum flow file count in a batch")
            .defaultValue("1000")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor WINDOW_LENGTH = new PropertyDescriptor
            .Builder().name("window-length")
            .displayName("Window Length(sn)")
            .defaultValue("300")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor HOP_LENGTH = new PropertyDescriptor
            .Builder().name("hop-length")
            .displayName("Hop Length(sn)")
            .defaultValue("60")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor GROUP_FIELDS  = new PropertyDescriptor
            .Builder().name("group-fields")
            .displayName("Group Fields")
            .description("Comma-separated group fields")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
      
    public static final PropertyDescriptor AGGREGATION_FIELDS  = new PropertyDescriptor
            .Builder().name("aggregation-fields")
            .displayName("Aggregation Fields")
            .description("Comma-separated aggregation fields")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    
    public static final PropertyDescriptor AGGREGATION_TYPE = new PropertyDescriptor.Builder()
            .name("aggregation-type")
            .displayName("Aggregation Type")
            .description("available values : count, sum and avg")
            .required(true)
            .defaultValue(AGG_COUNT_AV.getValue())
            //.allowableValues(AGG_COUNT_AV, AGG_AVG_AV, AGG_SUM_AV)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    private Set<Relationship> relationships;
  
    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    private List<PropertyDescriptor> descriptors;
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REDIS_CONNECTION_POOL);
        descriptors.add(WINDOW_LENGTH);
        descriptors.add(HOP_LENGTH);
        descriptors.add(GROUP_FIELDS);
        descriptors.add(AGGREGATION_TYPE);
        descriptors.add(AGGREGATION_FIELDS);
        descriptors.add(MAX_BATCH_SIZE);
        
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_AGGREGATED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    private volatile RedisConnectionPool redisConnectionPool;
    private String sessionUUID;
    private final String KEY_SEPARATOR = "|";
    private byte[] CALCULATION_LOCK = null;
    private final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    final ObjectMapper mapper = new ObjectMapper();
    private String calculationLuaScriptId;
    
    
    @OnScheduled
    public void setup(ProcessContext context) throws Exception{
    	getLogger().debug("setup");
    	this.redisConnectionPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPool.class);
    	this.sessionUUID = UUID.randomUUID().toString();
    	this.CALCULATION_LOCK = (sessionUUID + KEY_SEPARATOR + "calculation_lock").getBytes();
    	
    	withConnection(redisConnection -> {
			this.calculationLuaScriptId = redisConnection.scriptLoad(getScriptFile("calculate.lua"));
    		return null;
    	});
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {	
    	final ComponentLog logger = getLogger();
    	final Integer batchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
    	final Long windowLength = context.getProperty(WINDOW_LENGTH).evaluateAttributeExpressions().asLong();
    	final Long hopLength = context.getProperty(HOP_LENGTH).evaluateAttributeExpressions().asLong();
    	final String aggregationType = context.getProperty(AGGREGATION_TYPE).evaluateAttributeExpressions().getValue();
    	final String groupFields = context.getProperty(GROUP_FIELDS).evaluateAttributeExpressions().getValue(); 
    	//final String aggFields = aggregationType.equals(AGG_COUNT) ? groupFields : context.getProperty(AGGREGATION_FIELDS).evaluateAttributeExpressions().getValue();
    	final String aggFields = context.getProperty(AGGREGATION_FIELDS).evaluateAttributeExpressions().getValue();
    	
    	
    	final List<FlowFile> flowFiles = session.get(batchSize);
        for (FlowFile flowFile : flowFiles) {
        	try 
        	{
        		Map<byte[], byte[]> hashes = new HashMap<byte[], byte[]>();
        		byte[] cacheKey = (sessionUUID +KEY_SEPARATOR+flowFile.getAttribute("uuid")).getBytes();
        		StringBuilder groupKeyBuilder = new StringBuilder();
        		
        		// read flow file
        		session.read(flowFile, new InputStreamCallback() {
				     @Override
				     public void process(InputStream in) throws IOException {
				    	 String jsonStr = IOUtils.toString(in, "UTF-8");
				    	 logger.debug(jsonStr);
				    	 Map<String, Object> jsonMap = mapper.readValue(jsonStr, new TypeReference<Map<String,Object>>(){});
				    	 populateHashes(jsonMap, hashes, aggFields);
				    	 populateGroupKey(jsonMap, groupFields, groupKeyBuilder);
				     }
				});
       
        		// prepare caches for the flow, put the required data into suitable cache group
        		withConnection(redisConnection -> {
        			
        			redisConnection.hMSet(cacheKey, hashes);
        			redisConnection.expire(cacheKey, windowLength);
        			
        			byte[] groupKey = groupKeyBuilder.toString().getBytes();
        			redisConnection.sAdd(groupKey, cacheKey);
        			redisConnection.expire(groupKey,windowLength);
        			
        			byte[] mainKey = getMainKey();
        			redisConnection.sAdd(mainKey, groupKey);
        			redisConnection.expire(mainKey,windowLength);
        		
    	            return null;
    	        });
        		session.transfer(flowFile, REL_ORIGINAL);
        		 
            }catch(Exception e) {
            	logger.error("", e);
            }
        }
        
        // make calculation on redis cache 
        
        try 
    	{
	        withConnection(redisConnection -> {
	        	
	        	Boolean calculationLockAcquired = redisConnection.setNX(CALCULATION_LOCK, CALCULATION_LOCK);
	        	if (calculationLockAcquired)
	        	{
	        		redisConnection.expire(CALCULATION_LOCK, hopLength);
	        		
		        	logger.debug("aggregation-type="+aggregationType);
		        	logger.debug("aggregation-group-keys="+groupFields);
		        	
		        	byte[] mainKey = getMainKey();  	
		        	Set<byte[]> groups = redisConnection.sMembers(mainKey);
		        	
		        	// traverse groups
	    			for (byte[]  group : groups) {
	    				
	    				Map<String, String> aggregationMap = null;
	    				List<byte[]> l = redisConnection.evalSha(calculationLuaScriptId, ReturnType.MULTI, 1, group);
	    				
	    				if (l == null || l.size() == 0) {
	    					// remove group
	    					redisConnection.watch(group);
	    					redisConnection.multi();
	    					redisConnection.sRem(mainKey, group);
	    					redisConnection.exec(); // if one was added to this group by another thread while removing, do not remove 
	    					continue;
	    				}
	    				
    					aggregationMap = new HashMap<String, String>();
    					for(int i=0; i<l.size()-1; i=i+2) {
    						aggregationMap.put(new String(l.get(i)), new String(l.get(i+1)));
    					}
	    				logger.debug(aggregationMap.toString());
	    				
	    				String aggregationName = new String(group);
	        			logger.debug("aggregation-group-name="+aggregationName);
	        			Map<String, Object> dataNode = new LinkedHashMap<String, Object>();
	        			String[] aggregationNamesplitted = aggregationName.split("\\|");
    					String[] groupFieldsSplit = groupFields.split(",");
    					
    					if (groupFieldsSplit.length != (aggregationNamesplitted.length -1) ) {
    						// corrupted data, continue
    						continue;
    					}
    					for (int i = 0; i < groupFieldsSplit.length; i++) {
    						dataNode.put(groupFieldsSplit[i], aggregationNamesplitted[i+1]);
						}
	    				
    					if (aggFields!=null && ! aggFields.equalsIgnoreCase("")) {
    						String[] aggFieldsSplit = aggFields.split(",");
    						for (int i = 0; i < aggFieldsSplit.length; i++) {
    							String aggField = aggFieldsSplit[i];
    							BigDecimal count = new BigDecimal(aggregationMap.get(aggField+".count"));
    							BigDecimal sum = new BigDecimal(aggregationMap.get(aggField+".sum"));
    							
    							 if (aggregationType.equals(AGG_COUNT)) {  // if aggregation fields are presented in a count calculation use sum instead of count 
		 	    					logger.debug("aggregation-count="+sum);
		 	    					dataNode.put("count", sum);
		 	        			 }else if (aggregationType.equals(AGG_AVG)) {
		 	        				BigDecimal avg = sum.divide(count);
		 	        				logger.debug("aggregation-avg="+ avg);
		 	        				dataNode.put(aggField + "_avg", avg);
		 	        			 }else if (aggregationType.equals(AGG_SUM)) {
		 	        				logger.debug("aggregation-sum="+sum);
		 	        				dataNode.put(aggField + "_sum", sum);
		 	        			 }		 
							}
    					}else { // if aggregation fields are not presented in a count calculation use count 
    						BigDecimal count = new BigDecimal(aggregationMap.get(DEFAULT_COUNT_KEY+".count"));
							logger.debug("aggregation-count="+count);
 	    					dataNode.put("count", count);
    					}
    					
    					// add dates
	        			LocalDateTime now = LocalDateTime.now();
	        			dataNode.put("begin_date", now.minusSeconds(windowLength).format(DateTimeFormatter.ofPattern(DATE_FORMAT)));
	        			dataNode.put("end_date", now.format(DateTimeFormatter.ofPattern(DATE_FORMAT)));
	        			//create flow
	    			    createFlowfile(session, mapper.writeValueAsString(dataNode), aggregationNamesplitted);
    					
					}
	        	}
	            return null;
	        });
    	}catch(Exception e) {
         	logger.error("", e);
        }
        
    	
    }
    
    public void createFlowfile(ProcessSession session, String json, String[] aggregationNamesplitted) {
    	 FlowFile ff = session.create();
         ff = session.write(ff, out -> {
         	out.write(json.getBytes(StandardCharsets.UTF_8));
         });
         StringBuilder aggregationId = new StringBuilder();
         for (int i = 1; i< aggregationNamesplitted.length; i++) {
        	 aggregationId.append(aggregationNamesplitted[i]);
         }
         ff = session.putAttribute(ff, "aggregation-id", Base64.getEncoder().encodeToString(aggregationId.toString().getBytes()));
         session.transfer(ff, REL_AGGREGATED);
    }
    
    public void populateHashes(Map<String, Object> jsonMap, Map<byte[], byte[]> hashes, String aggFields) { 	
    	if (aggFields == null || aggFields.equalsIgnoreCase("")) {
    		 hashes.put(DEFAULT_COUNT_KEY.getBytes(), DEFAULT_COUNT_VALUE.getBytes());
    	}else {
    		String[] aggFieldsSplitted = aggFields.split(",");
        	for (String aggField : aggFieldsSplitted) {
        		hashes.put(aggField.getBytes(), jsonMap.get(aggField).toString().getBytes());
    		}
    	}
    }
    
   
    public StringBuilder  populateGroupKey(Map<String, Object> jsonMap, String groupFields, StringBuilder sb) {
    	sb.append(sessionUUID);
		String[] groupFieldsSplitted = groupFields.split(",");
    	for (int i = 0; i < groupFieldsSplitted.length; i++) {
    		sb.append(KEY_SEPARATOR);
    		sb.append(jsonMap.get(groupFieldsSplitted[i]));
		}	
    	return sb;
    }

    public byte[] getMainKey() {
    	StringBuilder sb = new StringBuilder();
    	sb.append(sessionUUID);
		sb.append(KEY_SEPARATOR);
		sb.append("main_key");
		return sb.toString().getBytes();
    }    
    
    public byte[] getScriptFile(String fn){
    	byte[] bytes = null;
		try {
			InputStream is = getClass().getClassLoader().getResourceAsStream(fn);
			bytes = IOUtils.toByteArray(is);
		} catch (Exception e) {
			getLogger().error("Can not get script file :"+fn ,e);
		}
        return bytes;
    }
    
    public <T> T withConnection(final RedisAction<T> action) throws IOException {
        RedisConnection redisConnection = null;
        try {
            redisConnection = redisConnectionPool.getConnection();
            return action.execute(redisConnection);
        } finally {
           if (redisConnection != null) {
               try {
                   redisConnection.close();
               } catch (Exception e) {
                   getLogger().warn("Error closing connection: " + e.getMessage(), e);
               }
           }
        }
    }
}