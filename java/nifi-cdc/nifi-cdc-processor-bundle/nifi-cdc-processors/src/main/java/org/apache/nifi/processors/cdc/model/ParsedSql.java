package org.apache.nifi.processors.cdc.model;

import java.util.ArrayList;
import java.util.List;

public class ParsedSql {
	private List<String> columns = new ArrayList<String>();
	private List<String> data = new ArrayList<String>();
	private List<String> before;
	
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	public List<String> getData() {
		return data;
	}
	public void setData(List<String> data) {
		this.data = data;
	}
	public List<String> getBefore() {
		return before;
	}
	public void setBefore(List<String> before) {
		this.before = before;
	}
	@Override
	public String toString() {
		return "{columns=" + columns + ", data=" + data + ", before=" + before + "}";
	}
}
