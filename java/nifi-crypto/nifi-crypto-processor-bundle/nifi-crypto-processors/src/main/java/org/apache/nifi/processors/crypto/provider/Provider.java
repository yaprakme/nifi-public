package org.apache.nifi.processors.crypto.provider;

import java.util.List;
import java.util.Map;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.crypto.model.FieldInfo;

public interface Provider {
    public static final String ENCRYPT = "encrypt";
	public static final String DECRYPT = "decrypt";
	public static final String HASH = "hash";
	    
	public static final String AES_CBC_PKCS5_PADDING = "AES/CBC/PKCS5Padding";
	public static final String PROVIDER = "SunJCE";
	public static final String HMAC_SHA256 = "HmacSHA256"; 
	public static final String AES = "AES";
	    

	public void init(List<FieldInfo> fis, String userName, String password, Map<String, String> dynamicPropertyMap, ComponentLog logger)  throws Exception;
	
}
