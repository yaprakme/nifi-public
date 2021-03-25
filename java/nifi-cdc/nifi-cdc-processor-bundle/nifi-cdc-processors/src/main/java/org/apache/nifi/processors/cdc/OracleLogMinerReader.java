package org.apache.nifi.processors.cdc;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.cdc.model.ParsedSql;
import org.apache.nifi.processors.cdc.util.SqlParser;

import com.fasterxml.jackson.databind.ObjectMapper;



/**
 * This processor makes use of Oracle Logminer to trace database events for specific tables
 */
@TriggerSerially
@Tags({"cdc", "oracle", "reader", "logminer"})
@CapabilityDescription("Trace Oracle database events for specific tables")
@Stateful(description = "'startScn' holds every iteration's the last scn for next iteration so that processor continues from where it left when it was restarted", scopes = { Scope.CLUSTER })
public class OracleLogMinerReader extends AbstractProcessor {

   
    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to HDFS are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "Files that could not be written to HDFS for some reason are transferred to this relationship")
            .build();

    // properties
    public static final String TRX_STRATEGY_ALL = "all";
    public static final String TRX_STRATEGY_COMMITTED = "committed";
    public static final AllowableValue COMMITTED_TRX_AV = new AllowableValue(TRX_STRATEGY_COMMITTED, "Only committed transactions", "Only commited insert/update/delete");
    public static final AllowableValue ALL_TRX_AV = new AllowableValue(TRX_STRATEGY_ALL, "Commited and rollbacked transactions", "Commited and rollbacked insert/update/delete");
  
    
    public static final PropertyDescriptor MAX_FETCH_SIZE = new PropertyDescriptor
            .Builder().name("Max Fetch Size")
            .description("Maximum fetch size for each database trip")
            .defaultValue("1000")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor QUERY_TIMEOUT  = new PropertyDescriptor
            .Builder().name("Max Wait Time")
            .description("Maximum wait time (sn) for each database trip")
            .defaultValue("300")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor TABLE_NAMES  = new PropertyDescriptor
            .Builder().name("Table Names")
            .description("Table names prefixed with schema name for example : schema.table")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
      
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build(); 
    
    public static final PropertyDescriptor TRANSACTON_HANDLING = new PropertyDescriptor.Builder()
            .name("Transaction Handling Strategy")
            .description("Transaction handling strategy")
            .required(true)
            .defaultValue(ALL_TRX_AV.getValue())
            .allowableValues(COMMITTED_TRX_AV, ALL_TRX_AV)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            //.expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
 
    
    private Set<Relationship> relationships;
    private DBCPService dbcpService; 
    private volatile Map<String,List<String>> transactionMap;
    private Map<String, String> dynamicPropertyMap = null;
    
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(Validator.VALID)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
    	dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    	transactionMap = new HashMap<String,List<String>>();
    	
    	dynamicPropertyMap = new HashMap<String, String>();
		for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
	            PropertyDescriptor pd = property.getKey();
	            if (pd.isDynamic()) {
	                if (property.getValue() != null) {
	                	dynamicPropertyMap.put(pd.getName(), property.getValue());
	                }
	            }
	    } 
    }

    private List<PropertyDescriptor> descriptors;
  
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DBCP_SERVICE);
        descriptors.add(MAX_FETCH_SIZE);
        descriptors.add(QUERY_TIMEOUT);
        descriptors.add(TABLE_NAMES);
        descriptors.add(TRANSACTON_HANDLING);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {	
    	final ComponentLog logger = getLogger();
    	final Integer maxFetchSize = context.getProperty(MAX_FETCH_SIZE) == null ? 1000 : context.getProperty(MAX_FETCH_SIZE).asInteger();
    	final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT) == null ? 300 : context.getProperty(QUERY_TIMEOUT).asInteger();
    	final String trxHandlingStrategy = context.getProperty(TRANSACTON_HANDLING).getValue();
    	//final String dateFormat = context.getProperty(DATE_FORMAT).getValue();
    	
    	try (final Connection con = dbcpService.getConnection();)
    	{
    		final StateManager stateManager = context.getStateManager();
    		final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
    		
        	// alter dynamic session variables
    		try (Statement st = con.createStatement()) {	
    			for(Map.Entry<String, String> entry : dynamicPropertyMap.entrySet()) {
    				if (entry.getKey().startsWith("session")) {
    					 st.execute(entry.getValue());
    				}
    			}
    		}
        	
    		
    		DatabaseLogInfo dbLogInfo = getDatabaseLogInfo(con);
    		Long startScn = stateMap.get("startScn") == null ? dbLogInfo.getCurrentScn() : Long.parseLong(stateMap.get("startScn"));
         	List<LogFile> logFiles = getLogFiles(con, startScn);
         	addLogFile(con, logFiles);
         	startLogminer(con, trxHandlingStrategy, startScn, dbLogInfo.getCurrentScn());         	
         	Long lastScn = null;
         	if (trxHandlingStrategy.equals(TRX_STRATEGY_COMMITTED)) {
         		lastScn = proccessCommittedEvents(context, session, con, maxFetchSize, queryTimeout);
         	}else {
         		lastScn = proccessAllEvents(context, session, con, maxFetchSize, queryTimeout);
         	}
         	putNewState(lastScn != null ? lastScn : startScn, stateManager);
         	
        }catch(Exception e) {
        	logger.error("", e);
        	session.rollback();
        }
    }
    
    
    
    public Long proccessAllEvents(ProcessContext context, ProcessSession session, Connection con, int maxFetchSize, int queryTimeout) throws Exception{
    	final ComponentLog logger = getLogger();
    	final List<FlowFile> resultSetFlowFiles = new ArrayList<>();
    	int totalResultSetCount = 0;
     	ObjectMapper mapper = new ObjectMapper();
     	Long scn = null;  
     	
     	String sql = "SELECT XID,SQL_REDO,SCN,TIMESTAMP,OPERATION,SEG_OWNER,SEG_NAME,ROW_ID FROM v$logmnr_contents WHERE ( OPERATION_CODE in (1,2,3) and " + getWhereStatementForContent(context) +")";
    	logger.debug("proccessAllEvents-ExecutedSql = {}", new Object[] {sql});
    	try (PreparedStatement st = con.prepareStatement(sql)) {
    		st.setFetchSize(maxFetchSize);
    		st.setQueryTimeout(queryTimeout);
    		st.executeQuery();
         	
    		final ResultSet rs = st.getResultSet();
    		
         	while (rs.next()) {
         		totalResultSetCount ++;
         		
         		String segName = rs.getString("SEG_NAME");
         		String segOwner = rs.getString("SEG_OWNER");
         		String operation = rs.getString("OPERATION").toLowerCase();   
         		scn = rs.getLong("SCN");
         		String redo = rs.getString("SQL_REDO");
         		String timeStamp = rs.getString("TIMESTAMP");
         		String rowId = rs.getString("ROW_ID");
         		
         		String json = mapper.writeValueAsString(populate(segOwner, segName, operation, null, redo, scn, timeStamp, rowId));
         		
         		
                FlowFile resultSetFF = session.create();
                resultSetFF = session.write(resultSetFF, out -> {
                	out.write(json.getBytes(StandardCharsets.UTF_8));
                });
                resultSetFlowFiles.add(resultSetFF);
                
                if (resultSetFlowFiles.size() >= maxFetchSize) {
                	session.transfer(resultSetFlowFiles, REL_SUCCESS);
                	session.commit();
                	logger.debug("proccessAllEvents- {} flowfiles were created after {} rows were fetched from result set.", new Object[] {resultSetFlowFiles.size(), maxFetchSize});
                	resultSetFlowFiles.clear(); 
                	
                }
        	}
         	
         	 // Transfer any remaining files to SUCCESS
            session.transfer(resultSetFlowFiles, REL_SUCCESS);
         	logger.debug("proccessAllEvents-ExecutedSqlResultSize = {}", new Object[] {totalResultSetCount});
    	}     	
    	return scn == null ? null : (scn + 1);
    }
    
    public Long proccessCommittedEvents(ProcessContext context, ProcessSession session, Connection con, int maxFetchSize, int queryTimeout) throws Exception{
    	final ComponentLog logger = getLogger();
    	int transferredFlowFileCount = 0;
     	int totalResultSetCount = 0;
     	ObjectMapper mapper = new ObjectMapper();
     	Long scn = null;
     	
     	String sql = "SELECT XID,SQL_REDO,SCN,TIMESTAMP,OPERATION,SEG_OWNER,SEG_NAME,ROW_ID FROM v$logmnr_contents WHERE (OPERATION_CODE in (7,36)) OR (ROLLBACK = 0 and OPERATION_CODE in (1,2,3) and " + getWhereStatementForContent(context) +")";
    	logger.debug("proccessCommittedEvents-ExecutedSql = {}", new Object[] {sql});
    	try (PreparedStatement st = con.prepareStatement(sql)) {
    		st.setFetchSize(maxFetchSize);
    		st.setQueryTimeout(queryTimeout);
    		st.executeQuery();
         		
    		final ResultSet rs = st.getResultSet();
    		  
         	while (rs.next()) {
         		totalResultSetCount ++;
         		
         		String segName = rs.getString("SEG_NAME");
         		String segOwner = rs.getString("SEG_OWNER");
         		String operation = rs.getString("OPERATION").toLowerCase(); 
         		String xid = rs.getString("XID");
         		String redo = rs.getString("SQL_REDO");
         		scn = rs.getLong("SCN");
         		String timeStamp = rs.getString("TIMESTAMP");
         		String rowId = rs.getString("ROW_ID");
         
         		if ((operation.equals("insert"))||(operation.equals("update"))||(operation.equals("delete"))){ 
         					
         			String transaction = mapper.writeValueAsString(populate(segOwner,segName, operation, xid, redo, scn, timeStamp, rowId));
         			
         			List<String> transactions = transactionMap.get(xid);
         			if (transactions == null) {
         				transactions = new ArrayList<String>();
         				transactions.add(transaction);
         			}else {
         				transactions.add(transaction);
         			}
         			transactionMap.put(xid, transactions);	
         			
         		}else if(operation.equals("commit")) {
         			List<String> transactions = transactionMap.get(xid);
         			if (transactions != null) {
         				for (String transaction : transactions) {
         					 FlowFile resultSetFF = session.create();
                             resultSetFF = session.write(resultSetFF, out -> {
                             	out.write(transaction.getBytes(StandardCharsets.UTF_8));
                             });
                             session.transfer(resultSetFF, REL_SUCCESS);
						}		
         				transferredFlowFileCount = transferredFlowFileCount + transactions.size();
         				transactionMap.remove(xid);
         			}
         		}else if (operation.equals("rollback")){
         			transactionMap.remove(xid);
         		}
        	}
         	logger.debug("proccessCommittedEvents-ExecutedSqlResultSize = {}", new Object[] {totalResultSetCount});
         	logger.debug("proccessCommittedEvents- {} flowfiles were transferred", new Object[] {transferredFlowFileCount});
         	logger.debug("proccessCommittedEvents-TransactionMap key count = {}", new Object[] {transactionMap.size()});     	
		}
    	return scn == null ? null : (scn + 1);
    }
    
    
    public Map<String, Object> populate(String segOwner, String segName, String operation, String xid, String redo, Long scn, String timeStamp, String rowId ) throws Exception {
     	Map<String, Object> dataNode = new LinkedHashMap<String, Object>();
     	
     	ParsedSql parsedSql = null;
		try {
			parsedSql = SqlParser.parse(redo);
		} catch (Exception e) {
			 getLogger().error("Sql parse error", e);
			parsedSql = new ParsedSql();
		}	
		
		List<Object> columnsNode = new ArrayList<Object>();
		List<String> columns = parsedSql.getColumns();
		List<String> data = parsedSql.getData();
		List<String> before = parsedSql.getBefore();
		for(int i=0; i<columns.size(); i++) {
			Map<String, Object> c = new LinkedHashMap<String, Object>();
			c.put("name", columns.get(i));
			if (data != null && data.size() > i) {
				List<Object> valueObj =  getValueOfType(data.get(i));
				c.put("value", valueObj.size() > 1 ? valueObj.get(0) : null );
				c.put("column_type",  valueObj.size() > 1 ? valueObj.get(1) : null);
			}else {
				c.put("value", null);
				c.put("column_type", null);
			}
			
			if (before != null && before.size() > i) {
				List<Object> beforeObj = getValueOfType(before.get(i));
				c.put("last_value", beforeObj.size() > 0 ? beforeObj.get(0) : null);
			}
			columnsNode.add(c);
		}

		dataNode.put("type", operation);
		dataNode.put("timestamp", timeStamp);
     	dataNode.put("table_name", segName);
     	dataNode.put("database", segOwner);
     	dataNode.put("row_id", rowId);
     	dataNode.put("trx_code", xid);
     	dataNode.put("seq", ""+scn);
     	dataNode.put("columns", columnsNode);
     	return dataNode;
    }
    
    public List<Object> getValueOfType(String value) {
    	List<Object> returnObj = new ArrayList<Object>();
    	try {
    	  BigDecimal bd = new BigDecimal(value);
    	  returnObj.add(bd) ;
    	  returnObj.add("number");
    	}catch(Exception e) {
    	  if (value != null && ( value.contains("TO_DATE('") || value.contains("TO_TIMESTAMP('")) )	{
    		  String[] tokens = value != null ? value.split("'") : null;
    		  if (tokens != null && tokens.length > 1) {
    			  returnObj.add(tokens[1]);
        		  returnObj.add("date");
    		  }else {
    			  returnObj.add(value);
        		  returnObj.add("varchar");
    		  }
    	  }else {
    		  returnObj.add(value);
    		  returnObj.add("varchar");
    	  }
    	}
    	return returnObj;
    }
    
    /**
     * put next scn into processor's cache to use it for next iteration
     */
    public void putNewState(long scn, StateManager stateManager) throws Exception{
     	final Map<String, String> newState = new HashMap<>(2);
     	newState.put("startScn",  ""+ (scn));
        stateManager.setState(newState, Scope.CLUSTER);
    }
    
    public String getWhereStatementForContent(ProcessContext context) {
    	 final String tableNames = context.getProperty(TABLE_NAMES).getValue();
    	 final List<String> tablesWithSchema = Arrays.asList(tableNames.split(","));
    	
    	 String selectWhereStmt="("; 
         for (String tableWithSchema:tablesWithSchema){
           List<String> tableWithSchemaSplitted = Arrays.asList(tableWithSchema.split("\\."));
           selectWhereStmt+="(SEG_OWNER='"+tableWithSchemaSplitted.get(0)+ "' AND SEG_NAME='"+tableWithSchemaSplitted.get(1)+ "') OR ";
         }        
         selectWhereStmt=selectWhereStmt.substring(0,selectWhereStmt.length()-4)+")";
         return selectWhereStmt;
    }
    
    
    public DatabaseLogInfo getDatabaseLogInfo(Connection con) throws Exception{	
    	final ComponentLog logger = getLogger();
    	
    	String sql = "SELECT ARCHIVELOG_CHANGE#, CURRENT_SCN, LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE";
    	logger.debug("getDatabaseLogInfo-ExecutedSql = "+sql);
    	
    	try (PreparedStatement st = con.prepareStatement(sql)) {
	   
         	st.execute();
         	final ResultSet rs = st.getResultSet();
         	rs.next();
         	
         	DatabaseLogInfo dli = new DatabaseLogInfo();
         	dli.setCurrentScn(rs.getLong("CURRENT_SCN"));
         	dli.setFistOnlineLogScn(rs.getLong("ARCHIVELOG_CHANGE#")); // online log begins where achieve log changes 
         	dli.setLogMode(rs.getString("LOG_MODE"));
         	dli.setSupplementalLogDataMin(rs.getString("SUPPLEMENTAL_LOG_DATA_MIN"));
         	logger.debug("getDatabaseLogInfo-ResultObject = "+dli.toString());
         	return dli;
		}
    }
    

    
    public List<LogFile> getLogFiles(Connection con, long scn) throws Exception{
    	final ComponentLog logger = getLogger();
    	List<LogFile> logFiles = new ArrayList<LogFile>();
    	List<LogFile> archiveLogFiles = new ArrayList<LogFile>();	
    	List<LogFile> onlineLogFiles = new ArrayList<LogFile>();
    	
    	String sql = "SELECT l.FIRST_CHANGE#, f.MEMBER FROM v$log l JOIN v$logfile f ON l.GROUP#= f.GROUP# WHERE l.FIRST_CHANGE# >=? OR ( (? BETWEEN l.FIRST_CHANGE# AND l.NEXT_CHANGE# ) AND (? != l.NEXT_CHANGE# ) ) ORDER BY l.FIRST_TIME DESC";
    	logger.debug("getLogFiles-ExecutedSql = {}, params=[{},{},{}]", new Object[] {sql, scn, scn, scn});
    	
    	try (PreparedStatement st = con.prepareStatement(sql)) {	
    		st.setLong(1, scn);
    		st.setLong(2, scn);
    		st.setLong(3, scn);
         	st.executeQuery();
   	
         	final ResultSet rs = st.getResultSet();   	
         	while (rs.next()) {
         		LogFile lf = new LogFile();
             	lf.setFirstChange(rs.getLong("FIRST_CHANGE#"));
             	lf.setMember(rs.getString("MEMBER"));
             	onlineLogFiles.add(lf);
         	}
         	logger.debug("getLogFiles-ExecutedSqlResultSize = {}", new Object[] {onlineLogFiles.size()}); 
		}
    	
    	logFiles.addAll(onlineLogFiles);
    	
    	long minScnOfOnlineLogs = logFiles.get(logFiles.size()-1).getFirstChange();
     	if (scn < minScnOfOnlineLogs) // if scn had been stayed behind online logs  check archives  
     	{	
     		sql = "SELECT a.FIRST_CHANGE#, a.NAME FROM V$ARCHIVED_LOG a WHERE ( a.FIRST_CHANGE# >=? AND a.NEXT_CHANGE# <= ? ) OR ( (? BETWEEN a.FIRST_CHANGE# AND a.NEXT_CHANGE# ) AND (? != a.NEXT_CHANGE# ) ) ORDER BY a.FIRST_TIME DESC";
        	logger.debug("getLogFiles-ExecutedSql = {}, params=[{},{},{},{},{}]", new Object[] {sql, scn, minScnOfOnlineLogs, scn, scn});
        	
        	try (PreparedStatement st = con.prepareStatement(sql)) {	
        		st.setLong(1, scn);
        		st.setLong(2, minScnOfOnlineLogs);
        		st.setLong(3, scn);
        		st.setLong(4, scn);
             	st.executeQuery();
             	
             	final ResultSet rs = st.getResultSet();   	
             	while (rs.next()) {
             		LogFile alf = new LogFile();
                 	alf.setFirstChange(rs.getLong("FIRST_CHANGE#"));
                 	alf.setMember(rs.getString("NAME"));
                 	archiveLogFiles.add(alf);
            	}
             	logger.debug("getLogFiles-ExecutedSqlResultSize = {}", new Object[] {archiveLogFiles.size()});
        	}
     	}
     	logFiles.addAll(archiveLogFiles);
     	return logFiles;
    }
    
    public void startLogminer(Connection con, String trxHandlingStrategy, long startScn, long endScn) throws Exception{
    	final ComponentLog logger = getLogger();
    	String sql = "BEGIN DBMS_LOGMNR.START_LOGMNR( STARTSCN  => ?, ENDSCN   => ?,  OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +  DBMS_LOGMNR.SKIP_CORRUPTION"+ ( trxHandlingStrategy.equals(TRX_STRATEGY_COMMITTED) ? " + DBMS_LOGMNR.NO_ROWID_IN_STMT " : "" ) +" ); END;";
    	logger.debug("startLogminer-ExecutedSql = {}  params=[{},{}]", new Object[] {sql, startScn, endScn});
    	
    	try (CallableStatement st = con.prepareCall(sql)) {
    		st.setLong(1, startScn);
    		st.setLong(2, endScn);
         	st.execute();
		}
    }
    
    
    public void addLogFile(Connection con, List<LogFile> onlineLogFiles) throws Exception{
    	final ComponentLog logger = getLogger();
		for(int i=0; i<onlineLogFiles.size(); i++) {
			
			String sql = null;
			if (i==0) {  // insert first one as 'new' so that logminer ends previous session and starts new one 
				sql = "begin DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?, OPTIONS => DBMS_LOGMNR.NEW); END;";
			}else {
				sql = "begin DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?, OPTIONS => DBMS_LOGMNR.ADDFILE); END;";
			}
			
	    	logger.debug("addLogFile-ExecutedSql = {}  params=[{}]", new Object[] {sql, onlineLogFiles.get(i).getMember()});
			try (CallableStatement st = con.prepareCall(sql)) {
				st.setString(1, onlineLogFiles.get(i).getMember());   
				st.execute();
			}
		}   
    }
     
    // MODELS
      
    class LogFile {
    	private long firstChange;
    	private String member;
    	
		public long getFirstChange() {
			return firstChange;
		}
		public void setFirstChange(long firstChange) {
			this.firstChange = firstChange;
		}
		public String getMember() {
			return member;
		}
		public void setMember(String member) {
			this.member = member;
		}
		@Override
		public String toString() {
			return "{firstChange=" + firstChange + ", member=" + member + "}";
		}
    }
    
    class DatabaseLogInfo {
    	private long currentScn;
    	private long fistOnlineLogScn;  
    	private String logMode;
    	private String supplementalLogDataMin;
    	
		public long getFistOnlineLogScn() {
			return fistOnlineLogScn;
		}
		public void setFistOnlineLogScn(long fistOnlineLogScn) {
			this.fistOnlineLogScn = fistOnlineLogScn;
		}
		public long getCurrentScn() {
			return currentScn;
		}
		public void setCurrentScn(long currentScn) {
			this.currentScn = currentScn;
		}
		public String getLogMode() {
			return logMode;
		}
		public void setLogMode(String logMode) {
			this.logMode = logMode;
		}
		public String getSupplementalLogDataMin() {
			return supplementalLogDataMin;
		}
		public void setSupplementalLogDataMin(String supplementalLogDataMin) {
			this.supplementalLogDataMin = supplementalLogDataMin;
		}
		@Override
		public String toString() {
			return "{currentScn=" + currentScn + ", fistOnlineLogScn=" + fistOnlineLogScn + ", logMode="
					+ logMode + ", supplementalLogDataMin=" + supplementalLogDataMin + "}";
		}
    }
    
}