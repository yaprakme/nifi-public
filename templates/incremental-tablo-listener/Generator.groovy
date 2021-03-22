import org.apache.nifi.controller.ControllerService
import groovy.sql.Sql
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.components.state.Scope
 
 
 
def lookup = context.controllerServiceLookup
def dbServiceName = databaseConnectionPoolName.value
def dbcpServiceId = lookup.getControllerServiceIdentifiers(ControllerService).find {
	cs -> lookup.getControllerServiceName(cs) == dbServiceName
}
def conn = lookup.getControllerService(dbcpServiceId)?.getConnection()
 
def stateManager = context.stateManager
 
 
FlowFile flowFile = session.create();

try {
 
  def sql = new Sql(conn)
  def firstRow = sql.firstRow(sqlSelectMaxId.value)
  BigInteger maxIndex = firstRow[0]

 
  if (maxIndex == null){
	flowFile = session.putAttribute(flowFile, 'flowFileType', 'error')
	flowFile = session.putAttribute(flowFile, 'reason', 'maxIndex == null')
	session.transfer(flowFile, REL_SUCCESS)
	return;
  }
 
  BigInteger startIndex
  BigInteger startIndexInput = binding.hasVariable('StartIndexInput') ? new BigInteger(StartIndexInput.value) : null
  Integer partitionSize = new Integer(PartitionSize.value)
  
  def stateMap = stateManager.getState(Scope.LOCAL).toMap()
  if (stateMap.offsetId == null) {
	 if (startIndexInput != null){
		 startIndex = startIndexInput
	 }else{
		 startIndex = maxIndex - partitionSize
	 }
	 stateManager.setState(['offsetId': (startIndex +'')], Scope.LOCAL);
  }else {
	 startIndex = new BigInteger(stateMap.offsetId)
  }
 
  if (maxIndex < startIndex){
	flowFile = session.putAttribute(flowFile, 'flowFileType', 'error')
	flowFile = session.putAttribute(flowFile, 'reason', 'maxIndex < startIndex')
	session.transfer(flowFile, REL_SUCCESS)
	return;
  }
   
  if (maxIndex == startIndex){
	flowFile = session.putAttribute(flowFile, 'flowFileType', 'nonExecutableFlowFile')
	flowFile = session.putAttribute(flowFile, 'reason', 'maxIndex == startIndex')
	session.transfer(flowFile, REL_SUCCESS)
	return;
  }
 
 
  // EXECUTABLE FLOW FILE

  flowFile = session.putAttribute(flowFile, 'flowFileType', 'executableFlowFile')
  flowFile = session.putAttribute(flowFile, 'maxIndex', maxIndex.toString())
  flowFile = session.putAttribute(flowFile, 'startIndex', startIndex.toString())
  
  if (startIndex + partitionSize < maxIndex){
	maxIndex = startIndex + partitionSize
  }
  
  String sqlClause = sqlToExecute.value.replace(':prm1', (startIndex + 1) +'').replace(':prm2', maxIndex+'')
  flowFile = session.write(flowFile, {inputStream, outputStream ->
	   outputStream.write((sqlClause).getBytes(StandardCharsets.UTF_8))
  } as StreamCallback)
  
  // save last index
  stateManager.setState(['offsetId': (maxIndex +'')], Scope.LOCAL);
  
   
  
  session.transfer(flowFile, REL_SUCCESS)
 
 
  
} catch(e) {
	log.error('getMovements.groovy error', e)
	session.transfer(flowFile, REL_FAILURE)
}
finally
{
	conn?.close()
}

