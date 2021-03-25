package org.apache.nifi.processors.cdc.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processors.cdc.model.ParsedSql;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

public class SqlParser {
	
	public static ParsedSql  parse(String sql) throws Exception {
		ParsedSql data = new ParsedSql();
		String sqlRedo = sql.replace("IS NULL", "= NULL");
		
		Statement stmt = CCJSqlParserUtil.parse(sqlRedo);
		
     	if (stmt instanceof Insert) {
     		Insert insert = (Insert) stmt;
     		for (Column c : insert.getColumns()){
     			data.getColumns().add(clean(c.getColumnName()));
     		}
     		
     		ExpressionList eList = (ExpressionList) insert.getItemsList();
            List<Expression> valueList = eList.getExpressions();
            for (Expression expression : valueList) {		
				data.getData().add(clean(expression.toString()));
			} 
            
     	}else if (stmt instanceof Update) {
     		Map<String,String> mergedData =  new LinkedHashMap<String, String>(); // non-updated columns/values may be missing in logminer log, so that, use this map to merge before and current data
     		List<String> updatedColumns = new ArrayList<String>();
     		List<String> updatedValues = new ArrayList<String>();
     		Update update = (Update) stmt; 
     		
     		for (Column c : update.getColumns()){
     			updatedColumns.add(clean(c.getColumnName()));	
     		}
     		List<Expression> valueList = update.getExpressions();
     		for (Expression expression : valueList) {
     			updatedValues.add(clean(expression.toString()));
			}
	     		
 		    data.setBefore(new ArrayList<String>());
     		update.getWhere().accept(new ExpressionVisitorAdapter() {
                @Override
                public void visit(final EqualsTo expr){                    
                	String col = clean(expr.getLeftExpression().toString());
                    String value = clean(expr.getRightExpression().toString()); 
                    data.getBefore().add(value);
                    mergedData.put(col, value);
                }
            });
     		
     		// merge 'before' and 'data'(current) on 'mergedData' map
     		for (int i = 0; i < updatedColumns.size(); i++) {
     			mergedData.put( updatedColumns.get(i), updatedValues.get(i));
			}
     		// traverse through mergedData to calculate column names and data (current)
     		Iterator it = mergedData.entrySet().iterator();
     	    while (it.hasNext()) {
     	    	Map.Entry pair = (Map.Entry)it.next();
     	    	data.getColumns().add(pair.getKey().toString());
     	    	data.getData().add(pair.getValue().toString());
     	    }
     	
     	}else if (stmt instanceof Delete) {
     		 Delete delete = (Delete) stmt;
             delete.getWhere().accept(new ExpressionVisitorAdapter(){
               @Override
               public void visit(final EqualsTo expr){
                 String col = (expr.getLeftExpression().toString());
                 String value = (expr.getRightExpression().toString());             
                 data.getColumns().add(clean(col));
                 data.getData().add(clean(value));
               }          
             });
     	}
     	
		return data;		
	}
	
	
    public static String clean(String str) {                      
        if (str.startsWith("'") && str.endsWith("'")) {
        	str=str.substring(1,str.length()-1);        
        }
        if (str.startsWith("\"") && str.endsWith("\"") && str.length()>1) {
        	str=str.substring(1,str.length()-1);
        }
        return str;
      }       
}



