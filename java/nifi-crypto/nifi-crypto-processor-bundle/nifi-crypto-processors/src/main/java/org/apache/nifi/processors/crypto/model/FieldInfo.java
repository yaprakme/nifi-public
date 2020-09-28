package org.apache.nifi.processors.crypto.model;

public class FieldInfo {
	public String field;
	public String secret;
	public String operation;
	public String key;
	public Object crypto;

	public String getField() {
		return field;
	}
	public void setField(String field) {
		this.field = field;
	}
	public String getSecret() {
		return secret;
	}
	public void setSecret(String secret) {
		this.secret = secret;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Object getCrypto() {
		return crypto;
	}
	public void setCrypto(Object crypto) {
		this.crypto = crypto;
	}
	
	@Override
	public String toString() {
		return "FieldInfo [field=" + field + ", secret=" + secret + ", operation=" + operation + ", key=" + key
				+ ", crypto=" + crypto + "]";
	}
}
