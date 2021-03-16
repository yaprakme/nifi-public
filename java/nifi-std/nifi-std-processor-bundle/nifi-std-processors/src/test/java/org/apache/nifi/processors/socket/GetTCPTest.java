
package org.apache.nifi.processors.socket;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class GetTCPTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GetTCP.class);
    }

    @Test
    public void localMockTest() throws Exception {
    	
    	testRunner.setValidateExpressionUsage(false);
    	testRunner.setProperty(GetTCP.SOCKET_CONNECT_HOST, "localhost");
    	testRunner.setProperty(GetTCP.SOCKET_CONNECT_PORT, "9000");
    	testRunner.setProperty(GetTCP.MAX_FLOW_OUTPUT, "2");
    	testRunner.setProperty(GetTCP.MAX_SOCKET_BUFFER_SIZE, "4 B");
    	testRunner.setProperty(GetTCP.LOGON_PHRASE, "LOGON\n");
    	//testRunner.setProperty(MatriksStreamListener.TYPE_FILTER, "068,069");
    
    
        //testRunner.setRunSchedule(10000);
        testRunner.run(1);

        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(GetTCP.REL_SUCCESS);
        List<MockFlowFile> fails = testRunner.getFlowFilesForRelationship(GetTCP.REL_FAILURE);
        
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
