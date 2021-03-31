import org.apache.nifi.controller.ControllerService
import groovy.sql.Sql
import org.apache.commons.io.IOUtils
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.json.JsonGenerator
import java.nio.charset.StandardCharsets
import java.util.HashMap
import java.util.Map
import java.util.List
import java.util.ArrayList
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.io.StreamCallback;



Integer maxBatchSize = binding.hasVariable('MaxBatchSize') ? new Integer(MaxBatchSize.value) : 1000
def jsonGenerator = new JsonGenerator.Options().disableUnicodeEscaping().build()



flowFileList = session.get(maxBatchSize)
if(!flowFileList.isEmpty()) {
   flowFileList.each { flowfile ->
// start flowFileList loop
	   
def contentJson = null
String fileName = ""
String fileContent = null
String text = null

try {
	
   flowfile = session.write(flowfile, {inputStream, outputStream ->
	   text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
	   contentJson = new JsonSlurper().parseText(text)
	} as StreamCallback)
   
   
   fileName = contentJson.row_id
   
   def dataNode = new LinkedHashMap()
   for (int i=0; i<contentJson.columns.size(); i++) {
	   Object c = contentJson.columns[i]
	   dataNode.put(c.name, c.value)
   }
   

   // create data flow file
   FlowFile ff = session.create();
   ff = session.write(ff, {inputStream, outputStream ->
		  outputStream.write(jsonGenerator.toJson(dataNode).getBytes(StandardCharsets.UTF_8))
	 } as StreamCallback)
   ff = session.putAttribute(ff, 'operation', ""+ (contentJson.type=='delete'?'delete':'insert'))
   ff = session.putAttribute(ff, 'filename', ""+ fileName)
   session.transfer(ff, REL_SUCCESS)
   //
   
   
} catch (Exception e) {
	log.error("",e)
}
finally
{
	session.transfer(flowfile, REL_FAILURE)
}


// end of flowFileList list
}
}



// utilities

Object getKeyValues(contentJson, kc) {
	for (int i=0; i<contentJson.columns.size(); i++) {
		String c = contentJson.columns[i]
		if (c.equalsIgnoreCase(kc) ) {
			return  contentJson.data[i]
		}
	}
	return null
}

void info(s){
	log.info('HdfsPrepare '+s)
}