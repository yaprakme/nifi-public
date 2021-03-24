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

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.nifi.pulsar.StandardPulsarClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestReaderPulsar {
	   
	private TestRunner testRunner;
	
	@Before
    public void init() throws InitializationException {
        testRunner = TestRunners.newTestRunner(ReaderPulsar.class);
    }
	
	@Test
    public void testReaderWithInitialMessageId() throws InitializationException {
		testRunner.setValidateExpressionUsage(false);
		final PulsarClientService service = new StandardPulsarClientService();
		testRunner.addControllerService("myService", service);
		//testRunner.setProperty(service, StandardPulsarClientService.PULSAR_SERVICE_URL, "192.168.0.11:6650");
		testRunner.setProperty(service, StandardPulsarClientService.PULSAR_SERVICE_URL, "192.168.43.162:6650");
		testRunner.enableControllerService(service);
		
		// configure the processor and link it with the service
		testRunner.setProperty(ReaderPulsar.PULSAR_CLIENT_SERVICE, "myService");
		testRunner.setProperty(ReaderPulsar.TOPIC, "nifi-test");
		testRunner.setProperty(ReaderPulsar.READ_TIMEOUT, "60 sec");
		testRunner.setProperty(ReaderPulsar.RESULTSET_MAX_MESSAGES, "100");
		testRunner.setProperty(ReaderPulsar.INITIAL_MESSAGE_ID, "COoJEAEgAA=="); 
		
		testRunner.run(1);
		
		List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
	    System.out.println("-------- succeeded flow size = "+success.size()+" ---------");
		printMockFlowFile(success);
	}
	
	@Test
    public void testReaderWithoutInitialMessageId() throws InitializationException {
		testRunner.setValidateExpressionUsage(false);
		final PulsarClientService service = new StandardPulsarClientService();
		testRunner.addControllerService("myService", service);
		testRunner.setProperty(service, StandardPulsarClientService.PULSAR_SERVICE_URL, "192.168.43.162:6650");
		//testRunner.setProperty(service, StandardPulsarClientService.PULSAR_SERVICE_URL, "192.168.0.11:6650");
		testRunner.enableControllerService(service);
		
		// configure the processor and link it with the service
		testRunner.setProperty(ReaderPulsar.PULSAR_CLIENT_SERVICE, "myService");
		testRunner.setProperty(ReaderPulsar.TOPIC, "nifi-test");
		testRunner.setProperty(ReaderPulsar.READ_TIMEOUT, "60 sec");
		testRunner.setProperty(ReaderPulsar.RESULTSET_MAX_MESSAGES, "3");
 
		testRunner.setRunSchedule(5000);
		testRunner.run(6);
		
		List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
	    System.out.println("-------- succeeded flow size = "+success.size()+" ---------");
		printMockFlowFile(success);
	}
	
   public void printMockFlowFile(List<MockFlowFile> flows) {
    	 try 
    	 {
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

}
