
package org.apache.nifi.processors.hadoop;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


/*
 * 
 * add HADOOP_HOME environment variable to run these methods
 *  
 */

public class HdfsWriterTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(HdfsWriter.class);
    }

    
    
    @Test
    public void testProcessor() throws InitializationException {
      
        testRunner.setProperty(HdfsWriter.DIRECTORY, "/test-dir");
        testRunner.setProperty(HdfsWriter.HADOOP_CONFIGURATION_RESOURCES, "D:\\hadoop-3.1.0\\etc\\hadoop\\core-site.xml,D:\\hadoop-3.1.0\\etc\\hadoop\\hdfs-site.xml");
        testRunner.setProperty(HdfsWriter.FLOW_FILE_BATCH_SIZE, "10");
        
        Map<String, String> m = new HashMap<String, String>();
        m.put("filename", "myfilename");
        //m.put("operation", "delete");
        m.put("operation", "insert");  // it inserts or updates
        
   	    // Add the content to the runner
        testRunner.enqueue("myfilecontent", m);
       
        testRunner.run(1);
        
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(HdfsWriter.REL_SUCCESS);
        List<MockFlowFile> fails = testRunner.getFlowFilesForRelationship(HdfsWriter.REL_FAILURE);
        System.out.println("success size = "+success.size());
        System.out.println("fails size = "+fails.size());

    }

    @Test
    public void testPathDelete() throws InitializationException {
      
        testRunner.setProperty(HdfsWriter.DIRECTORY, "/test-dir");
        testRunner.setProperty(HdfsWriter.HADOOP_CONFIGURATION_RESOURCES, "D:\\hadoop-3.1.0\\etc\\hadoop\\core-site.xml,D:\\hadoop-3.1.0\\etc\\hadoop\\hdfs-site.xml");
        testRunner.setProperty(HdfsWriter.OPERATION_TYPE, "PATH_DELETE");
        
        testRunner.run(1);
        
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(HdfsWriter.REL_SUCCESS);
        List<MockFlowFile> fails = testRunner.getFlowFilesForRelationship(HdfsWriter.REL_FAILURE);
        System.out.println("success size = "+success.size());
        System.out.println("fails size = "+fails.size());

    }
    
}
