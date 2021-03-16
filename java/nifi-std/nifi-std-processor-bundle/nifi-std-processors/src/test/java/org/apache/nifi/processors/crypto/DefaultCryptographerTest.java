
package org.apache.nifi.processors.crypto;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class DefaultCryptographerTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(DefaultCryptographer.class);
    }



    @Test
    public void testEncrptAndHash() throws Exception {
    	
    	testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(DefaultCryptographer.MAX_BULK_SIZE, "100");
        testRunner.setProperty(DefaultCryptographer.IV_SPEC, "125,0,12,1,0,40,33,90,0,97,0,101,0,2,0,101");
        
        testRunner.setProperty(DefaultCryptographer.FIELDS, "[{\"field\":\"customer_no\",\"secret\":\"muratemrahyaprak\",\"operation\":\"encrypt\"},{\"field\":\"card_no\",\"secret\":\"muratemrahyaprak\",\"operation\":\"encrypt\"},{\"field\":\"trx_amount\",\"secret\":\"yaprakemrahmurat\",\"operation\":\"hash\"}]");
              
        testRunner.enqueue("{\"customer_no\":\"1\",\"card_no\":\"cc11\",\"trx_amount\":100,\"trx_tax\":1.0}");
        testRunner.enqueue("{\"customer_no\":\"2\",\"card_no\":\"cc22\",\"trx_amount\":350,\"trx_tax\":3.5}");
       
        //testRunner.setRunSchedule(10000);
        testRunner.run(1); 
        
        print();
    }
    
    @Test
    public void testDecrpt() throws Exception {
    	
    	testRunner.setValidateExpressionUsage(false);
        testRunner.setProperty(DefaultCryptographer.MAX_BULK_SIZE, "100");
        testRunner.setProperty(DefaultCryptographer.IV_SPEC, "125,0,12,1,0,40,33,90,0,97,0,101,0,2,0,101");
        
        testRunner.setProperty(DefaultCryptographer.FIELDS, "[{\"field\":\"customer_no\",\"secret\":\"muratemrahyaprak\",\"operation\":\"decrypt\"},{\"field\":\"card_no\",\"secret\":\"muratemrahyaprak\",\"operation\":\"decrypt\"}]");
              
        testRunner.enqueue("{\"customer_no\":\"XVpnTeXcmxWbuoISu/WzDQ==\",\"card_no\":\"jnvQemC1iqvS4ys95W/OoA==\",\"trx_amount\":100,\"trx_tax\":1.0}");
        testRunner.enqueue("{\"customer_no\":\"doDhN+cvEihJUpC2YzwwTQ==\",\"card_no\":\"EJQ/Adse5AeEOEOWiAdyBw==\",\"trx_amount\":350,\"trx_tax\":3.5}");
       
        //testRunner.setRunSchedule(10000);
        testRunner.run(1); 
        
        print();
    }
    
    
    
    
    public void print() {
    	 List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(DefaultCryptographer.REL_SUCCESS);
         List<MockFlowFile> fails = testRunner.getFlowFilesForRelationship(DefaultCryptographer.REL_FAILURE);
         List<MockFlowFile> originals = testRunner.getFlowFilesForRelationship(DefaultCryptographer.REL_ORIGINAL);
         
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
