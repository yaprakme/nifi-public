package org.apache.nifi.processors.crypto.provider;

import java.util.List;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.crypto.model.FieldInfo;

public class SunJCE implements Provider{

	@Override
	public void init(List<FieldInfo> fis, String userName, String password, Map<String, String> dynamicPropertyMap, ComponentLog logger) throws Exception {
		
		for (FieldInfo fi : fis) {
			
			fi.key = fi.secret;
			
			if (fi.operation.equals(ENCRYPT)) {
				
				SecretKeySpec key = new SecretKeySpec(fi.secret.getBytes("UTF-8"), AES);
				Cipher encryptCipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING, PROVIDER);
				encryptCipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(fi.secret.getBytes("UTF-8")));
				fi.setCrypto(encryptCipher);
				
			}else if (fi.operation.equals(DECRYPT)) {
				
				SecretKeySpec key = new SecretKeySpec(fi.secret.getBytes("UTF-8"), AES);
				Cipher decryptCipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING, PROVIDER);
				decryptCipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(fi.secret.getBytes("UTF-8")));
				fi.setCrypto(decryptCipher);
				
			}else if (fi.operation.equals(HASH)) {
				
				byte[] keyBytes   =  fi.secret.getBytes("UTF-8");
				String algorithm  = "RawBytes";
				SecretKeySpec key = new SecretKeySpec(keyBytes, algorithm);
			    Mac mac = Mac.getInstance(HMAC_SHA256, PROVIDER);
			    mac.init(key);
			    fi.setCrypto(mac);
			    
			}
			
		}
		
	}

	
     
	
	
}
