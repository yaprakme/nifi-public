package org.apache.nifi.processors.crypto.provider;

import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.crypto.model.FieldInfo;

import com.ingrian.internal.ilc.IngrianLogService;
import com.ingrian.security.nae.IngrianProvider;
import com.ingrian.security.nae.NAEKey;
import com.ingrian.security.nae.NAESession;

public class Ingrian implements Provider{

	private static final String AES_CBC_PKCS5_PADDING = "AES/CBC/PKCS5Padding";
	private static final String INGRIAN_PROVIDER = "IngrianProvider";
	private static final String HMAC_SHA256 = "HmacSHA256";
	
	
	private NAESession session;
	
	@Override
	public void init(List<FieldInfo> fis, String userName, String password, Map<String, String> dynamicPropertyMap, ComponentLog logger) throws Exception {
		
		for(Map.Entry<String, String> entry : dynamicPropertyMap.entrySet()) {
			System.setProperty(entry.getKey(), entry.getValue());
		}
		
        IngrianLogService ingrianLogService = getIngrianLogService(logger);
         
		java.security.Security.addProvider(new IngrianProvider(ingrianLogService));
		
	    session = NAESession.getSession(userName, password.toCharArray());
	    
	
		for (FieldInfo fi : fis) {
			
			if (fi.operation.equals(ENCRYPT)) {
				SecretKey key = NAEKey.getSecretKey(fi.secret, session);
				Cipher encryptCipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING, INGRIAN_PROVIDER);
				encryptCipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(fi.secret.getBytes("UTF-8")));
				fi.setCrypto(encryptCipher);
				
			}else if (fi.operation.equals(DECRYPT)) {
				SecretKey key = NAEKey.getSecretKey(fi.secret, session);
				Cipher decryptCipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING, INGRIAN_PROVIDER);
				decryptCipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(fi.secret.getBytes("UTF-8")));
				fi.setCrypto(decryptCipher);
				
			}else if (fi.operation.equals(HASH)) {
				NAEKey key = NAEKey.getSecretKey(fi.secret, session);
				Mac mac = Mac.getInstance(HMAC_SHA256, INGRIAN_PROVIDER);
				mac.init(key);
				fi.setCrypto(mac);
			}	
		}
		
		session.closeSession();
		
	}

	public IngrianLogService getIngrianLogService(ComponentLog logger) {
		return new IngrianLogService() {
			
			@Override public void warn(String arg0, Throwable arg1) {}
			@Override public void warn(String arg0) {}
			@Override public void trace(String arg0) {}
			@Override public boolean isWarnEnabled() {return false;}
			@Override public boolean isTraceEnabled() {return false;}
			@Override public boolean isInfoEnabled() {return false;}
			
			@Override
			public boolean isErrorEnabled() {
	           return logger.isErrorEnabled();
			}
			
			@Override
			public boolean isDebugEnabled() {
				return logger.isDebugEnabled();
			}
			
			@Override public void info(String arg0) {}
			
			@Override
			public void error(String arg0, Throwable arg1) {
				logger.error(arg0, arg1);
			}
			
			@Override
			public void error(String arg0) {
				logger.error(arg0);
			}
			
			@Override
			public void debug(String arg0, Throwable arg1) {
				logger.debug(arg0, arg1);
			}
			
			@Override
			public void debug(String arg0) {
				logger.debug(arg0);
			}
		};
	}
     
	
	
}
