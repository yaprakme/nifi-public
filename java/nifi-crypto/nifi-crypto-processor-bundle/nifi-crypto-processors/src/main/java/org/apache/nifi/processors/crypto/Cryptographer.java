package org.apache.nifi.processors.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.Mac;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
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
import org.apache.nifi.processors.crypto.provider.Provider;
import org.apache.nifi.processors.crypto.provider.SunJCE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * 
 */
@Tags({"crypto", "cipher", "mac", "has"})
@CapabilityDescription("Crypto Utilities")
public class Cryptographer extends AbstractProcessor {

    final ObjectMapper mapper = new ObjectMapper();
    
    List<FieldInfo> fieldInfo;
    
   
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

    @OnScheduled
    public void setup(ProcessContext context) {
    	final ComponentLog logger = getLogger();
    	try {
    		
    		final String fields = context.getProperty(FIELDS).getValue();
    		fieldInfo = mapper.readValue(fields, new TypeReference<List<FieldInfo>>(){});
			Provider provider = new SunJCE();
			provider.crypto(fieldInfo);
			
			logger.debug(fieldInfo.toString());
			
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
					    			if (fi.getOperation().equals(Provider.ENCRYPT)) {
										contentMap.put(fi.field, encrypt((Cipher)fi.getCrypto(), contentMap.get(fi.getField()).toString(), fi.getSecret()));
									 }else if (fi.getOperation().equals(Provider.DECRYPT)) {
										 contentMap.put(fi.field, decrypt((Cipher)fi.getCrypto(), contentMap.get(fi.getField()).toString(), fi.getSecret())); 
									 }else if (fi.getOperation().equals(Provider.HASH)) {
										 contentMap.put(fi.field, hashText((Mac)fi.getCrypto(), contentMap.get(fi.getField()).toString(), fi.getSecret()));  
									 }	
								}		
						    } catch (Exception e) {
							  logger.error("", e);
						    }
				    	   
				    	   FlowFile ff = session.create();
				           ff = session.write(ff, out -> {
				           	out.write(mapper.writeValueAsString(contentMap).getBytes(StandardCharsets.UTF_8));
				           });
				           session.transfer(ff, REL_SUCCESS);
				    	  
				     }
				      
				});
        		
        		session.transfer(flowFile, REL_ORIGINAL);
        		 
			} catch (Exception e) {
				logger.error("onTrigger : ", e);
				session.transfer(flowFile, REL_FAILURE);
			}
        }	
    }
   
    public String encrypt(Cipher encryptCipher, String text, String secret)  throws Exception{
	    String encrypted = Base64.getEncoder().encodeToString(encryptCipher.doFinal(text.getBytes("UTF-8")));
	    return encrypted;	
    }

    public String decrypt(Cipher decryptCipher, String text, String secret) throws Exception{
		String decryptedStr = new String(decryptCipher.doFinal(Base64.getDecoder().decode(text)));
		return decryptedStr;
     }
    
    public String hashText(Mac mac, String text, String secret) throws Exception {
	    byte[] macValue = mac.doFinal(text.getBytes("UTF-8"));
	    return Base64.getEncoder().encodeToString(macValue);
	}
  
}