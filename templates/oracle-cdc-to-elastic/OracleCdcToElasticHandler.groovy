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
def stringBuilder = StringBuilder.newInstance()


flowFileList = session.get(maxBatchSize)
if(!flowFileList.isEmpty()) {
   flowFileList.each { flowfile ->
// start flowFileList loop
	   
def contentJson = null
String fileContent = null
String text = null

try {
	
   flowfile = session.write(flowfile, {inputStream, outputStream ->
	   text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
	   contentJson = new JsonSlurper().parseText(text)
	} as StreamCallback)
   
  
   
   def dataNode = new LinkedHashMap()
   for (int i=0; i<contentJson.columns.size(); i++) {
	   Object c = contentJson.columns[i]
	   dataNode.put(c.name, c.value)
   }
   
   
   if (contentJson.type=='delete') {
	   addBatch('delete', stringBuilder, getESName(contentJson.database) +'-'+ getESName(contentJson.table_name), contentJson.row_id, null)
   }else {
	   addBatch('index', stringBuilder, getESName(contentJson.database) +'-'+ getESName(contentJson.table_name), contentJson.row_id, jsonGenerator.toJson(dataNode))
   }
   
   
   
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


// write to flow file
if (stringBuilder.toString() != ''){
	createFlowFile(session, stringBuilder.toString())
}



// utilities

public String getESName(String s) {
	if (s == null) {
		return ""
	}
	return s.replaceAll("\\s|_|\\+|-|<|>|\"|\\*|/|\\?|\\\\", "")
			.toLowerCase()
}

void addBatch(type, stringBuilder, index, id, doc){
	stringBuilder.append("{\""+type+"\" : { \"_index\" : \""+index+"\", \"_id\" : \""+id+"\" } }")
	stringBuilder.append("\n")
	if (doc) {
	  stringBuilder.append(doc)
	  stringBuilder.append("\n")
	}
}

public void createFlowFile(session, data) {
	FlowFile ff = session.create();
	ff = session.write(ff, {inputStream, outputStream ->
	   outputStream.write(data.getBytes(StandardCharsets.UTF_8))
	} as StreamCallback)
	session.transfer(ff, REL_SUCCESS)
}

