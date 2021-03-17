
package org.apache.nifi.processors.fix;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class QuickFixInitiatorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(QuickFixInitiator.class);
    }

    @Test
    public void localTestOnAcceptor() throws Exception {
    	
    	testRunner.setValidateExpressionUsage(false);
    	testRunner.setProperty(QuickFixInitiator.BEGIN_STRING, "FIXT.1.1");
    	testRunner.setProperty(QuickFixInitiator.DEFAULT_APPL_VER_ID, "FIX.5.0SP2");
    	testRunner.setProperty(QuickFixInitiator.SOCKET_CONNECT_HOST, "localhost");
    	testRunner.setProperty(QuickFixInitiator.SOCKET_CONNECT_PORT, "9882");
    	testRunner.setProperty(QuickFixInitiator.SENDER_COMPONENT_ID, "NIFI");
    	testRunner.setProperty(QuickFixInitiator.TARGET_COMPONENT_ID, "BANZAI");
    	testRunner.setProperty(QuickFixInitiator.SENDER_SUB_ID, "FDA14");
    
    	
    	//testRunner.setProperty(QuickFixInitiator.START_TIME, "11:09:00 Europe/Istanbul");
    	//testRunner.setProperty(QuickFixInitiator.END_TIME, "11:11:00 Europe/Istanbul");
    	
    	testRunner.setProperty(QuickFixInitiator.RESET_SEQUENCE_NUMBER, "Y");
    	
    	
    	testRunner.setProperty(QuickFixInitiator.USERNAME, "user");
    	testRunner.setProperty(QuickFixInitiator.PASSWORD, "pass");
       
    	//testRunner.setProperty("AppDataDictionary", "FIX50SP2_custom.xml");
    	
        //testRunner.setRunSchedule(10000);
        testRunner.run(1);

        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(QuickFixInitiator.REL_SUCCESS);
        List<MockFlowFile> fails = testRunner.getFlowFilesForRelationship(QuickFixInitiator.REL_FAILURE);
        
        System.out.println("-------- succeeded flow size = "+success.size()+" ---------");
        printMockFlowFile(success);
        System.out.println("-------- failed flow size = "+fails.size()+" ---------");
        printMockFlowFile(fails);    
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
    
  
}
