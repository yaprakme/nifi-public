import org.apache.nifi.controller.ControllerService
import groovy.sql.Sql
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.components.state.Scope
import groovy.json.JsonOutput
import groovy.json.JsonGenerator
import java.util.List
import java.util.Map
import java.util.HashMap
import org.apache.nifi.processor.io.StreamCallback;



// database service lookup
def lookup = context.controllerServiceLookup
def dbServiceName = ConnectionName.evaluateAttributeExpressions().getValue()
def dbcpServiceId = lookup.getControllerServiceIdentifiers(ControllerService).find {
	cs -> lookup.getControllerServiceName(cs) == dbServiceName
}
// get connection
def conn = lookup.getControllerService(dbcpServiceId)?.getConnection()
conn.setNetworkTimeout(null, 10000)

// get processor state
def stateManager = context.stateManager
def stateMap = null
def captureInfo
String sqlClause = null
String startDate = null
String dateFormat = null
String maxPollSize = null


try {
	def sql = new Sql(conn)
	def jsonGenerator = new JsonGenerator.Options().disableUnicodeEscaping().build()
	
	stateMap = stateManager.getState(Scope.CLUSTER).toMap()
	startDate = binding.hasVariable('StartDate') ? StartDate.value : null
	dateFormat = binding.hasVariable('DateFormat') ? DateFormat.value : null
	maxPollSize = binding.hasVariable('MaxPollSize') ? MaxPollSize.value : '1000'
	String schema = Schema.evaluateAttributeExpressions().getValue()
	String table = Table.evaluateAttributeExpressions().getValue()
	

	captureInfo = getCaptureInfo(sql, schema, table, stateMap.offsetLongLsn)
	if (captureInfo == null){
		log.error('There is no capture to use. Please create capture')
		// close connection before return
		conn?.close()
		// clear state for new capture
		stateManager.clear(Scope.CLUSTER);
		
		return
	}
	
	if (stateMap.offsetHexLsn == null) {
		sqlClause = "SELECT top("+maxPollSize+") sys.fn_cdc_map_lsn_to_time(__\$start_lsn) as trx_time,* FROM cdc.fn_cdc_get_all_changes_"+captureInfo.capture_instance+" (sys.fn_cdc_map_time_to_lsn('smallest greater than','"+( (startDate!=null)?startDate:getFormattedStrDate(captureInfo.create_date,dateFormat) )+"'), sys.fn_cdc_get_max_lsn(), N'all update old')"
	}else {
		sqlClause = "SELECT top("+maxPollSize+") sys.fn_cdc_map_lsn_to_time(__\$start_lsn) as trx_time,* FROM cdc.fn_cdc_get_all_changes_"+captureInfo.capture_instance+" (sys.fn_cdc_increment_lsn("+stateMap.offsetHexLsn+"), sys.fn_cdc_get_max_lsn(), N'all update old')"
	}
	
	int startColumn=5,columnCount;
	def columnNames, columnsTypes
	def updatedValues = null, updatedLsn = null;
	
	def metaClosure = { meta ->
		columnCount = meta.columnCount
		columnNames = ((startColumn+1)..columnCount).collect{ meta.getColumnName(it) }
		columnsTypes = ((startColumn+1)..columnCount).collect{ meta.getColumnTypeName(it) }
	}
	def rowClosure = { row ->
		def values = []
		for (int i=startColumn; i<columnCount; i++){
			values.add(getValueOfType(columnsTypes[i-startColumn] , row[i], dateFormat))
		}
		//info(values)
	
		int operation = row[3]
		String lsn = encodeHex(row[1])
		
		if (operation in [1,2,4]) {
			
			// prepare data
			def dataNode = new LinkedHashMap()
			dataNode.columns=columnNames
			dataNode.types=columnsTypes
			dataNode.keyColumns=captureInfo.index_column_list
			dataNode.data=values
			if (operation == 4 ) {
				dataNode.before=(lsn.equals(updatedLsn)) ? updatedValues:null
			}else{
				dataNode.before=null
			}
			dataNode.operation=(operation == 1 ? "delete":(operation == 2 ? "insert":"update"))
			dataNode.trxDate=row[0].toString()
			dataNode.schema=schema
			dataNode.table=table
			dataNode.seq=lsn
			
			def dataJson = jsonGenerator.toJson(dataNode)
			
			// create data flow file
			FlowFile ff = session.create();
			ff = session.write(ff, {inputStream, outputStream ->
				   outputStream.write(dataJson.getBytes(StandardCharsets.UTF_8))
			  } as StreamCallback)
			ff = session.putAttribute(ff, 'lsn', lsn)
			ff = session.putAttribute(ff, 'trx_date', row[0].toString())
			ff = session.putAttribute(ff, 'capture_instance', captureInfo.capture_instance)
			session.transfer(ff, REL_SUCCESS)
			
			// save state
			stateManager.setState(['offsetHexLsn': lsn, 'offsetLongLsn': new BigInteger(row[1]).toString() ], Scope.CLUSTER);
			
		}else if (operation == 3){
			updatedValues=values
			updatedLsn=lsn
			// do nothing, wait for update 4
		}
		
	}
	sql.eachRow(sqlClause, metaClosure, rowClosure)

} catch(e) {
	if (e instanceof java.sql.SQLException && e.getErrorCode() == 313) {
		log.info('Waiting for Event - '+ (stateMap.offsetHexLsn!=null?("Last commited lsn:"+stateMap.offsetHexLsn):("Capture["+captureInfo.capture_instance+"] started with date["+captureInfo.create_date+"]")) )
	}else {
	   log.error('MsSqlChangeDataCapture '+(sqlClause?("sql["+sqlClause+"]") : ""), e)
	}
}
finally
{
	conn?.close()
}


Object getCaptureInfo(sql, schema, table, offsetLsn){
	List rows = null
	Map object = new HashMap()
	String sqlClause = "EXECUTE sys.sp_cdc_help_change_data_capture @source_schema = N'"+schema+"', @source_name = N'"+table+"'"
	try{
	   rows = sql.rows(sqlClause)
	   if (rows.size() == 1) { // there is one capture, use it
			object = createCaptureObject(rows[0])
	   }else if (rows.size() == 2){ // there are two capture, rows[0] == old, row[1] == new
		  if (offsetLsn == null){ // initial cycle, use newest capture
			object = createCaptureObject(rows[1])
		  }else {
			  BigInteger bOffsetLsn = new BigInteger(offsetLsn)
			  BigInteger captureStartLsn =  rows[1].start_lsn ? new BigInteger(rows[1].start_lsn) : null
			  if (bOffsetLsn && captureStartLsn && (bOffsetLsn.compareTo(captureStartLsn) == 0 || bOffsetLsn.compareTo(captureStartLsn) == 1)){  //  try to use newest capture if it meets offset
				 object = createCaptureObject(rows[1])
			  }else {
				  // new capture has not reached the offset yet so use old capture
				  object = createCaptureObject(rows[0])
			  }
		  }
	   }else {
		   log.error("Please check the 'Capture'. There must be up to 2 'Capture' for "+schema+"."+table)
		   return null
	   }
	}catch(Exception e) {
		if (e instanceof java.sql.SQLException && e.getErrorCode() != 22985) {
			log.error(""+sqlClause, e)
		}else {
			log.error("", e)
		}
		return null
	}
	log.info("SelectedCapture: " + object)
	return object
}

Object createCaptureObject(row) {
	Map object = new HashMap()
	object.capture_instance = row.capture_instance
	object.index_column_list = getIndexArray(row.index_column_list)
	object.create_date = row.create_date
	return object
}

String getFormattedStrDate(dateObj,dateFormat) {
	String formattedDate = null
	try{
		if (dateFormat) {
			Date date = new Date();
			date.setTime(dateObj.getTime());
			formattedDate = new java.text.SimpleDateFormat(dateFormat).format(date)
			//info("formattedDate="+formattedDate)
			return formattedDate;
		}
	}catch(Exception e) {
		log.error("date format error", e)
	}
	return formattedDate;
}

Object getIndexArray(columnStr){
	def values = null
	if (columnStr != null && ! columnStr.equals("")){
		values = []
		def splitted = columnStr.split(',',-1)
		splitted.each { s ->
			s = s.trim()
			values.add(s.substring(1,s.length()-1))
		}
	}
	return values
}

String getValueOfType(type, value, dateFormat) {
	
	if (value == null) {
		return "null"
	}
	
	if (type.equalsIgnoreCase("binary")  // btye[] comes
		|| type.equalsIgnoreCase("varbinary") // btye[] comes
		|| type.equalsIgnoreCase("image") // btye[] comes
	) {
		  return encodeHex(value)
	  }
	 
	  else if (type.equalsIgnoreCase("datetime")
		  || type.equalsIgnoreCase("date")
		  || type.equalsIgnoreCase("time"))
	  {
		  return getFormattedStrDate(value,dateFormat)
	  }
	  else {
		  return value.toString()
	  }
	 //
}

String encodeHex(byteArray){
	return '0x'+byteArray.encodeHex().toString()
}

void info(s){
	log.info('MsSqlChangeDataCapture '+s)
}
