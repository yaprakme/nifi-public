
package org.apache.nifi.processors.cdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


/*
 * oracle logminer cdc test
 * oracle jdbc driver must be added to junit classpath (add external jar) to run
 */
public class OracleLogMinerReaderTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(OracleLogMinerReader.class);
    }

    @Test
    public void testProcessor() throws Exception {
    	testRunner.setValidateExpressionUsage(false);
    	
    	final DBCPService dbcp =  new DBCPServiceSimpleImpl();
    	final Map<String, String> dbcpProperties = new HashMap<>();
        testRunner.addControllerService("dbcp", dbcp, dbcpProperties);
        testRunner.enableControllerService(dbcp);
        testRunner.setProperty(OracleLogMinerReader.DBCP_SERVICE, "dbcp");
    
        testRunner.setProperty(OracleLogMinerReader.MAX_FETCH_SIZE, "2");
        testRunner.setProperty(OracleLogMinerReader.QUERY_TIMEOUT, "300");
        testRunner.setProperty(OracleLogMinerReader.TABLE_NAMES, "LOGMINER.PERSON,LOGMINER.DEPT");
 
        
        //testRunner.setProperty(OracleLogMinerReader.TRANSACTON_HANDLING, OracleLogMinerReader.TRX_STRATEGY_COMMITTED);
        testRunner.setProperty(OracleLogMinerReader.TRANSACTON_HANDLING, OracleLogMinerReader.TRX_STRATEGY_ALL);
        
        
        // dynamic properties
        testRunner.setProperty("session1", "alter session set nls_date_format='yyyy-MM-dd\"T\"HH24:MI:SS'");
        testRunner.setProperty("session2", "alter session set nls_timestamp_format='yyyy-MM-dd\"T\"HH24:MI:SS'");
       
        testRunner.setRunSchedule(10000);
        testRunner.run(5); 
        
        
        
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(OracleLogMinerReader.REL_SUCCESS);
        List<MockFlowFile> fails = testRunner.getFlowFilesForRelationship(OracleLogMinerReader.REL_FAILURE);
        
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
    
    /**
     * Simple DBCP implementation only for testing.
     *
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                final Connection con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/orcl","logminer","logminer");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
      } 
    
    
    
}
