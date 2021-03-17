
package org.apache.nifi.processors.cdc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;

import org.apache.nifi.processors.redis.TimeWindow;
import org.apache.nifi.processors.redis.util.RedisUtils;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.RedisType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import redis.embedded.RedisServer;


public class TimeWindowTest {

    private TestRunner testRunner;
    private RedisServer redisServer;
    private int redisPort = 6379;
    
    @Before
    public void init() {
        try {
        	//https://github.com/kstyrc/embedded-redis
			redisServer = new RedisServer(redisPort);
			redisServer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
        testRunner = TestRunners.newTestRunner(TimeWindow.class);
    }

    @Test
    public void testProcessor() throws Exception {
       
    	testRunner.setValidateExpressionUsage(false);
    	RedisConnectionPool redisConnectionPool  = new RedisConnectionPoolServiceTestImpl();
    	testRunner.addControllerService("redis-connection-pool", redisConnectionPool);
    	testRunner.setProperty(redisConnectionPool, RedisUtils.CONNECTION_STRING, "localhost:" + redisPort);
    	testRunner.enableControllerService(redisConnectionPool);
    	testRunner.setProperty(TimeWindow.REDIS_CONNECTION_POOL, "redis-connection-pool");
    	testRunner.setProperty(TimeWindow.MAX_BATCH_SIZE, "6");
    	testRunner.setProperty(TimeWindow.AGGREGATION_TYPE, TimeWindow.AGG_AVG_AV);
    	testRunner.setProperty(TimeWindow.AGGREGATION_FIELDS, "trx_amount,trx_tax");
    	
    	testRunner.setProperty(TimeWindow.GROUP_FIELDS, "customer_no,card_no");
    	testRunner.setProperty(TimeWindow.WINDOW_LENGTH, "10");
    	testRunner.setProperty(TimeWindow.HOP_LENGTH, "3");
    	
        testRunner.enqueue("{\"customer_no\":\"1\",\"card_no\":\"cc11\",\"trx_amount\":100,\"trx_tax\":1.0}");
        testRunner.enqueue("{\"customer_no\":\"1\",\"card_no\":\"cc11\",\"trx_amount\":150,\"trx_tax\":1.5}");
        testRunner.enqueue("{\"customer_no\":\"1\",\"card_no\":\"cc12\",\"trx_amount\":200,\"trx_tax\":2.0}");
        testRunner.enqueue("{\"customer_no\":\"2\",\"card_no\":\"cc21\",\"trx_amount\":250,\"trx_tax\":2.5}");
        testRunner.enqueue("{\"customer_no\":\"2\",\"card_no\":\"cc21\",\"trx_amount\":300,\"trx_tax\":3.0}");
        testRunner.enqueue("{\"customer_no\":\"2\",\"card_no\":\"cc22\",\"trx_amount\":350,\"trx_tax\":3.5}");
        //testRunner.setRunSchedule(4000);
        //testRunner.run(2); 
        testRunner.run(1);
    	
        
        List<MockFlowFile> original = testRunner.getFlowFilesForRelationship(TimeWindow.REL_ORIGINAL);
        List<MockFlowFile> fails = testRunner.getFlowFilesForRelationship(TimeWindow.REL_FAIL);
        List<MockFlowFile> aggregated = testRunner.getFlowFilesForRelationship(TimeWindow.REL_AGGREGATED);
        System.out.println("-------- original flow size = "+original.size()+" ---------");
        printMockFlowFile(original);
        System.out.println("-------- aggregated flow size = "+aggregated.size()+" ---------");
        printMockFlowFile(aggregated);  
        System.out.println("-------- failed flow size = "+fails.size()+" ---------");
        printMockFlowFile(fails);     
        
    }

    
    
    @Test
    public void testLua() throws Exception {
   
    	RedisConnectionPool redisConnectionPool  = new RedisConnectionPoolServiceTestImpl();
    	testRunner.addControllerService("redis-connection-pool", redisConnectionPool);
    	testRunner.setProperty(redisConnectionPool, RedisUtils.CONNECTION_STRING, "localhost:" + redisPort);
    	testRunner.enableControllerService(redisConnectionPool);
    	
    	Map<byte[], byte[]> hash1 = new HashMap<byte[], byte[]>();
    	hash1.put("a".getBytes(), "1".getBytes());
    	hash1.put("b".getBytes(), "2".getBytes());
    	hash1.put("c".getBytes(), "1".getBytes());
    	Map<byte[], byte[]> hash2 = new HashMap<byte[], byte[]>();
    	hash2.put("a".getBytes(), "11".getBytes());
    	hash2.put("b".getBytes(), "22".getBytes());
    	hash2.put("c".getBytes(), "1".getBytes());
    	Map<byte[], byte[]> hash3 = new HashMap<byte[], byte[]>();
    	hash3.put("a".getBytes(), "111".getBytes());
    	hash3.put("b".getBytes(), "222".getBytes());
    	hash3.put("c".getBytes(), "1".getBytes());
    	
    	redisConnectionPool.getConnection().hMSet("f1".getBytes(), hash1);
    	redisConnectionPool.getConnection().hMSet("f2".getBytes(), hash2);
    	redisConnectionPool.getConnection().hMSet("f3".getBytes(), hash3);
    	redisConnectionPool.getConnection().sAdd("group1".getBytes(), "f1".getBytes());
    	redisConnectionPool.getConnection().sAdd("group1".getBytes(), "f2".getBytes());
    	redisConnectionPool.getConnection().sAdd("group1".getBytes(), "f3".getBytes());
    	
    	String scriptId = redisConnectionPool.getConnection().scriptLoad(get("calculate.lua"));
    
    	List<byte[]> list = redisConnectionPool.getConnection().evalSha(scriptId, ReturnType.MULTI, 1, "group1".getBytes());
    	//List<byte[]> list = redisConnectionPool.getConnection().eval(get("calculate.lua"), ReturnType.MULTI, 1, "group1".getBytes());
    	for (byte[] bs : list) {
			System.out.println(new String(bs));
		}
    }

    
    
    
    
    public byte[] get(String fn) throws Exception{
    	InputStream is = getClass().getClassLoader().getResourceAsStream(fn);
        byte[] bytes = IOUtils.toByteArray(is);
        return bytes;
    }
    
    
    public void printMockFlowFile(List<MockFlowFile> flows) {
    	 try {
			for (int i = 0; i < flows.size(); i++) {
			 	System.out.println("---- flow "+(i+1)+"----");
			 	MockFlowFile result = flows.get(i);
			    System.out.println(IOUtils.toString(testRunner.getContentAsByteArray(result), "UTF-8"));
			    System.out.println(flows.get(i).getAttributes());
			 }
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    public class RedisConnectionPoolServiceTestImpl extends AbstractControllerService implements RedisConnectionPool {

        private volatile PropertyContext context;
        private volatile RedisType redisType;
        private volatile JedisConnectionFactory connectionFactory;

        @Override
        protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return RedisUtils.REDIS_CONNECTION_PROPERTY_DESCRIPTORS;
        }

        @Override
        protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
            return RedisUtils.validate(validationContext);
        }

        @OnEnabled
        public void onEnabled(final ConfigurationContext context) {
            this.context = context;

            final String redisMode = context.getProperty(RedisUtils.REDIS_MODE).getValue();
            this.redisType = RedisType.fromDisplayName(redisMode);
        }

        @OnDisabled
        public void onDisabled() {
            if (connectionFactory != null) {
                connectionFactory.destroy();
                connectionFactory = null;
                redisType = null;
                context = null;
            }
        }

        @Override
        public RedisType getRedisType() {
            return redisType;
        }

        @Override
        public RedisConnection getConnection() {
            if (connectionFactory == null) {
                synchronized (this) {
                    if (connectionFactory == null) {
                        connectionFactory = RedisUtils.createConnectionFactory(context, getLogger());
                    }
                }
            }
            return connectionFactory.getConnection();
        }
    }

    @After
    public void teardown() throws IOException {
        if (redisServer != null) {
            redisServer.stop();
        }
     }
}
