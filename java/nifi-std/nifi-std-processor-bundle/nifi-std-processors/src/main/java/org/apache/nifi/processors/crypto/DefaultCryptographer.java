package org.apache.nifi.processors.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.spec.KeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.crypto.model.FieldInfo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Bytes;


/**
 * 
 */
@Tags({"SunJCE", "crypto", "cipher", "mac", "hash"})
@CapabilityDescription("Crypto Utilities")
public class DefaultCryptographer extends AbstractProcessor {

    final ObjectMapper mapper = new ObjectMapper();
    
    List<FieldInfo> fieldInfo;
    
    
	private static final String AES_CBC_PKCS5_PADDING = "AES/CBC/PKCS5Padding";
	private static final String HMAC_SHA256 = "HmacSHA256";
	
	public static final String ENCRYPT = "encrypt";
	public static final String DECRYPT = "decrypt";
	public static final String HASH = "hash";
	
	
	
	
    // relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully handled are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "Files that could not be handled for some reason are transferred to this relationship")
            .build();
    
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original flow files are transferred to this relationship")
            .build();

    
    public static final PropertyDescriptor MAX_BULK_SIZE = new PropertyDescriptor
            .Builder().name("Max Bulk Size")
            .description("Max Bulk Size")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor IV_SPEC = new PropertyDescriptor.Builder()
            .name("Iv Spec")
            .description("comma-seperated string")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    
    public static final PropertyDescriptor FIELDS = new PropertyDescriptor
            .Builder().name("Fields Info")
            .description("Fields Info")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    private Set<Relationship> relationships;

    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    
    Map<String, String> dynamicPropertyMap = null;
    		


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

    @OnScheduled
    public void setup(ProcessContext context) {
    	final ComponentLog logger = getLogger();
    
    	try {
    	
    		 dynamicPropertyMap = new HashMap<String, String>();
    		 for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
    	            PropertyDescriptor pd = property.getKey();
    	            if (pd.isDynamic()) {
    	                if (property.getValue() != null) {
    	                	dynamicPropertyMap.put(pd.getName(), property.getValue());
    	                }
    	            }
    	    } 
    		logger.debug("DynamicProperties"+dynamicPropertyMap.toString()); 
    	
    		
			
		} catch (Exception e) {
			logger.error("setup : ", e);
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
        descriptors.add(MAX_BULK_SIZE);
        descriptors.add(FIELDS);
        descriptors.add(IV_SPEC);
   
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {	
    	final ComponentLog logger = getLogger();
    	final Integer maxBulkSize = context.getProperty(MAX_BULK_SIZE).asInteger();
    	// get crypto descriptions
		final String fields = context.getProperty(FIELDS).getValue();
		final String ivSpec = context.getProperty(IV_SPEC).getValue();
		
		try {
		
				fieldInfo = mapper.readValue(fields, new TypeReference<List<FieldInfo>>(){});
		
		    	for(Map.Entry<String, String> entry : dynamicPropertyMap.entrySet()) {
					System.setProperty(entry.getKey(), entry.getValue());
				}
						
				IvParameterSpec ivs = getIvParameterSpec(ivSpec);
			    logger.debug("got Iv Spec:"+ivs.toString()+" for :"+ivSpec);
		    	
		    	final List<FlowFile> flowFiles = session.get(maxBulkSize);
		        for (FlowFile flowFile : flowFiles) {
			    	try {
						
			    		// read flow file
		        		session.read(flowFile, new InputStreamCallback() {
						     @Override
						     public void process(InputStream in) throws IOException {
			 
						    		String contentStr = IOUtils.toString(in, "UTF-8");
							    	logger.debug(contentStr);
							    	Map<String, Object> contentMap = mapper.readValue(contentStr, new TypeReference<Map<String,Object>>(){});
							    	
							    	try {  		
							    		for (FieldInfo fi : fieldInfo) {
							    			if (fi.getOperation().equals(ENCRYPT)) {
							    				SecretKeySpec secretKey = generateSecretKeySpec(fi.secret, fi.secret);
							    				Cipher encryptCipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING);
							    				encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey, ivs);
												contentMap.put(fi.field, encrypt(encryptCipher, contentMap.get(fi.getField()).toString()));
											 }else if (fi.getOperation().equals(DECRYPT)) {
												 SecretKeySpec secretKey = generateSecretKeySpec(fi.secret, fi.secret);
												 Cipher decryptCipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING);
												 decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, ivs);
												 contentMap.put(fi.field, decrypt(decryptCipher, contentMap.get(fi.getField()).toString())); 
											 }else if (fi.getOperation().equals(HASH)) {
												 Key sk = new SecretKeySpec(fi.secret.getBytes(), HMAC_SHA256);
												 Mac mac = Mac.getInstance(sk.getAlgorithm());
												 mac.init(sk);
												 contentMap.put(fi.field, hashText(mac, contentMap.get(fi.getField()).toString()));  
											 }	
										}
							    		
							    		 
							    	   FlowFile ff = session.create();
							           ff = session.write(ff, out -> {
							           	out.write(mapper.writeValueAsString(contentMap).getBytes(StandardCharsets.UTF_8));
							           });
							           session.transfer(ff, REL_SUCCESS);
								           
								    } catch (Exception e) {
									  logger.error("", e);
								    }
						    	   
						     }
						      
						});
		        		
		        		session.transfer(flowFile, REL_ORIGINAL);
		        		 
					} catch (Exception e) {
						logger.error("onTrigger-onFlowfile : ", e);
						session.transfer(flowFile, REL_FAILURE);
					}
		        }
		        
		        
		} catch (Exception e) {
			logger.error("onTrigger : ", e);
		}     
    }
   
    public String encrypt(Cipher encryptCipher, String text)  throws Exception{
	    String encrypted = Base64.getEncoder().encodeToString(encryptCipher.doFinal(text.getBytes("UTF-8")));
	    return encrypted;	
    }

    public String decrypt(Cipher decryptCipher, String text) throws Exception{
		String decryptedStr = new String(decryptCipher.doFinal(Base64.getDecoder().decode(text)));
		return decryptedStr;
     }
    
    public String hashText(Mac mac, String text) throws Exception {
    	final byte[] hmac = mac.doFinal(text.getBytes());
		StringBuilder sb = new StringBuilder(hmac.length * 2);
		Formatter formatter = new Formatter(sb);
		for (byte b : hmac) {
			formatter.format("%02x", b);
		}
		formatter.close();
		String hashedValue = sb.toString();
		return hashedValue;
	}
    

	public static IvParameterSpec getIvParameterSpec(String ivSpec) throws Exception {
		List<Byte> list = new ArrayList<Byte>();
		if (ivSpec != null && !ivSpec.equals("")) {
			String[] splittedIvSpec = ivSpec.split(",");
			for (String byteVal : splittedIvSpec) {
				list.add(Byte.parseByte(byteVal));
			}
			return new IvParameterSpec(Bytes.toArray(list));
		} else {
			byte[] iv = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
			return new IvParameterSpec(iv);
		}
	}
	
	private static final String HMAC_SHA256_ALGORITHM = "PBKDF2WithHmacSHA256";
	private static final String AES_ALGORITHM = "AES";
	private static final int KEY_LENGHT = 256;
	private static final int ITERATION_COUNT = 65536;

	Map<String, SecretKeySpec> keyCache = new HashMap<String, SecretKeySpec>();
	
	public  SecretKeySpec generateSecretKeySpec(String key, String salt) throws Exception {
		if (keyCache.get(key) == null) {
			SecretKeyFactory factory = SecretKeyFactory.getInstance(HMAC_SHA256_ALGORITHM);
	        KeySpec spec = new PBEKeySpec(key.toCharArray(), salt.getBytes(), ITERATION_COUNT, KEY_LENGHT);
			SecretKey tmp = factory.generateSecret(spec);
			SecretKeySpec secretKeySpec = new SecretKeySpec(tmp.getEncoded(), AES_ALGORITHM);
			keyCache.put(key, secretKeySpec);
			return secretKeySpec;
		}else {
			return keyCache.get(key);
		}
	}
  
}