
package org.apache.nifi.processors.crypto;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class CryptographerTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(Cryptographer.class);
    }

    @Test
    public void testProcessor() throws Exception {
    	
        testRunner.setProperty(Cryptographer.MAX_BULK_SIZE, "100");
        testRunner.setProperty(Cryptographer.FIELDS, "[{\"field\":\"customer_no\",\"secret\":\"muratemrahyaprak\",\"operation\":\"encrypt\"},{\"field\":\"card_no\",\"secret\":\"muratemrahyaprak\",\"operation\":\"encrypt\"},{\"field\":\"trx_amount\",\"secret\":\"yaprakemrahmurat\",\"operation\":\"hash\"}]");
        
        //testRunner.setProperty(Cryptographer.FIELDS, "customer_no,card_no");
        //testRunner.setProperty(Cryptographer.KEY_NAMES, "muratemrahyaprak,yaprakemrahmurat");
        //testRunner.setProperty(Cryptographer.CRYPTO_TYPE, Cryptographer.ENCRYPT_AV);
        
        
        testRunner.enqueue("{\"customer_no\":\"1\",\"card_no\":\"cc11\",\"trx_amount\":100,\"trx_tax\":1.0}");
        testRunner.enqueue("{\"customer_no\":\"2\",\"card_no\":\"cc22\",\"trx_amount\":350,\"trx_tax\":3.5}");
       
        //testRunner.setProperty(Cryptographer.CRYPTO_TYPE, Cryptographer.DECRYPT_AV);
        //testRunner.enqueue("{\"customer_no\":\"OdQ6n8qQ/TJYfR6sCZbi4Q==\",\"card_no\":\"iM/oCwIpstz+KPEpKLxFIA==\",\"trx_amount\":100,\"trx_tax\":1.0}");
       
        
        //testRunner.setRunSchedule(10000);
        testRunner.run(1); 
        
        
        
        
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Cryptographer.REL_SUCCESS);
        List<MockFlowFile> fails = testRunner.getFlowFilesForRelationship(Cryptographer.REL_FAILURE);
        List<MockFlowFile> originals = testRunner.getFlowFilesForRelationship(Cryptographer.REL_ORIGINAL);
        
        System.out.println("-------- succeeded flow size = "+success.size()+" ---------");
        printMockFlowFile(success);
        System.out.println("-------- failed flow size = "+fails.size()+" ---------");
        printMockFlowFile(fails);
        System.out.println("-------- original flow size = "+originals.size()+" ---------");
        printMockFlowFile(originals);
        
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
