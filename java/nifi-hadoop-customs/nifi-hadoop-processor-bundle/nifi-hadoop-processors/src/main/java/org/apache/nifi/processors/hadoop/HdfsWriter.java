
package org.apache.nifi.processors.hadoop;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.util.AbstractHadoopProcessor;
import org.apache.nifi.processors.hadoop.util.HadoopValidators;
import org.apache.nifi.stream.io.StreamUtils;

/**
 * This processor copies FlowFiles to HDFS.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "HDFS", "put", "copy", "filesystem"})
@CapabilityDescription("Write FlowFile data to Hadoop Distributed File System (HDFS)")
@ReadsAttributes({
	    @ReadsAttribute(attribute = "filename", description = "The name of the file written to HDFS comes from the value of this attribute."),
	    @ReadsAttribute(attribute = "operation", description = "The name of the operaton to perfom. Options are insert and delete")
})

@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file written to HDFS is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file on HDFS is stored in this attribute.")
})

@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
        explanation = "Provides operator the ability to delete any file that NiFi has access to in HDFS or the local filesystem.")
})
public class HdfsWriter extends AbstractHadoopProcessor {

   
    public static final String BUFFER_SIZE_KEY = "io.file.buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 4096;
    public static final String FILE_CRUD = "FILE_CRUD";
    public static final String PATH_DELETE = "PATH_DELETE";
    public static final AllowableValue FILE_CRUD_AV = new AllowableValue(FILE_CRUD, "File Operations", "File Operations");
    public static final AllowableValue PATH_DELETE_AV = new AllowableValue(PATH_DELETE, "Path Delete", "Path Delete");

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
    
    
    public static final PropertyDescriptor OPERATION_TYPE = new PropertyDescriptor.Builder()
            .name("Operation Type")
            .description("Operation Type")
            .required(true)
            .defaultValue(FILE_CRUD_AV.getValue())
            .allowableValues(FILE_CRUD_AV, PATH_DELETE_AV)
            .build();

    public static final PropertyDescriptor BLOCK_SIZE = new PropertyDescriptor.Builder()
            .name("Block Size")
            .description("Size of each block as written to HDFS. This overrides the Hadoop Configuration")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("IO Buffer Size")
            .description("Amount of memory to use to buffer file contents during IO. This overrides the Hadoop Configuration")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor REPLICATION_FACTOR = new PropertyDescriptor.Builder()
            .name("Replication")
            .description("Number of times that HDFS will replicate each file. This overrides the Hadoop Configuration")
            .addValidator(HadoopValidators.POSITIVE_SHORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor UMASK = new PropertyDescriptor.Builder()
            .name("Permissions umask")
            .description(
                   "A umask represented as an octal number which determines the permissions of files written to HDFS. " +
                           "This overrides the Hadoop property \"fs.permissions.umask-mode\".  " +
                           "If this property and \"fs.permissions.umask-mode\" are undefined, the Hadoop default \"022\" will be used.")
            .addValidator(HadoopValidators.UMASK_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOTE_OWNER = new PropertyDescriptor.Builder()
            .name("Remote Owner")
            .description(
                    "Changes the owner of the HDFS file to this value after it is written. This only works if NiFi is running as a user that has HDFS super user privilege to change owner")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor REMOTE_GROUP = new PropertyDescriptor.Builder()
            .name("Remote Group")
            .description(
                    "Changes the group of the HDFS file to this value after it is written. This only works if NiFi is running as a user that has HDFS super user privilege to change group")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IGNORE_LOCALITY = new PropertyDescriptor.Builder()
            .name("Ignore Locality")
            .displayName("Ignore Locality")
            .description(
                    "Directs the HDFS system to ignore locality rules so that data is distributed randomly throughout the cluster")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor FLOW_FILE_BATCH_SIZE = new PropertyDescriptor
            .Builder().name("Flow File Batch Size")
            .description("Flow File Batch Size")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
      
    
    private static final Set<Relationship> relationships;

    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(DIRECTORY)
                .description("The parent HDFS directory to which files should be written. The directory will be created if it doesn't exist.")
                .build());
        
        props.add(OPERATION_TYPE);
        props.add(FLOW_FILE_BATCH_SIZE);
        props.add(BLOCK_SIZE);
        props.add(BUFFER_SIZE);
        props.add(REPLICATION_FACTOR);
        props.add(UMASK);
        props.add(REMOTE_OWNER);
        props.add(REMOTE_GROUP);
        props.add(COMPRESSION_CODEC);
        props.add(IGNORE_LOCALITY);
        return props;
    }

    @Override
    protected void preProcessConfiguration(final Configuration config, final ProcessContext context) {
        // Set umask once, to avoid thread safety issues doing it in onTrigger
        final PropertyValue umaskProp = context.getProperty(UMASK);
        final short dfsUmask;
        if (umaskProp.isSet()) {
            dfsUmask = Short.parseShort(umaskProp.getValue(), 8);
        } else {
            dfsUmask = FsPermission.getUMask(config).toShort();
        }
        FsPermission.setUMask(config, new FsPermission(dfsUmask));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	
    	final Integer flowBatchSize = context.getProperty(FLOW_FILE_BATCH_SIZE).asInteger();
    	final String operationType = context.getProperty(OPERATION_TYPE).getValue();
    	final String dirValue = context.getProperty(AbstractHadoopProcessor.DIRECTORY).evaluateAttributeExpressions().getValue();
    	final Path configuredRootDirPath = new Path(dirValue);
    	
   
        final FileSystem hdfs = this.getFileSystem();
        final Configuration configuration = this.getConfiguration();
        final UserGroupInformation ugi = this.getUserGroupInformation();
        if (configuration == null || hdfs == null || ugi == null) {
            this.getLogger().error("HDFS not configured properly");
            context.yield();
            return;
        }
        
        try {
			if (!hdfs.getFileStatus(configuredRootDirPath).isDirectory()) {
				this.getLogger().error(configuredRootDirPath.toString() + " already exists and is not a directory");	
				context.yield();
	            return;
				  
			} 
		} catch (Exception e1) {
			try {
				if (!hdfs.mkdirs(configuredRootDirPath)) {
				    throw new IOException(configuredRootDirPath.toString() + " could not be created");
				 }
			} catch (Exception e) {
				e.printStackTrace();
			}  
		}
        
        
        try {
			if (operationType.equals(PATH_DELETE)) {
				hdfs.delete(configuredRootDirPath, true); 
				return;
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
        
        
        final long blockSize =  hdfs.getDefaultBlockSize(configuredRootDirPath);
        final int bufferSize = configuration.getInt("io.file.buffer.size", 4096);
        final short replication =  hdfs.getDefaultReplication(configuredRootDirPath);
        
        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
            	
            	
            	final List<FlowFile> putFlowFiles = session.get(flowBatchSize);
                for (FlowFile putFlowFile : putFlowFiles) {
            	
		                try {  
		                    
		                   
		                    final String filename = putFlowFile.getAttribute(CoreAttributes.FILENAME.key());
		                    final String operation = putFlowFile.getAttribute("operation");
		                    
		                    final Path copyFile = new Path(configuredRootDirPath, filename);
		               
		                    if (operation.equalsIgnoreCase("delete")) {
		                    	final boolean destinationExists = hdfs.exists(copyFile);
			                    if (destinationExists) {                  	
			                    	hdfs.delete(copyFile, false);      	
			                    }
		                    }else {		              
				                    session.read(putFlowFile, (InputStreamCallback)new InputStreamCallback() {
				                        public void process(final InputStream in) throws IOException {
				                            OutputStream fos = null;
				                            Path createdFile = null;
				                            try {
				                               
				                                final EnumSet<CreateFlag> cflags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
				                               
				                                fos = hdfs.create(copyFile, FsCreateModes.applyUMask(FsPermission.getFileDefault(), FsPermission.getUMask(hdfs.getConf())), (EnumSet<CreateFlag>)cflags, bufferSize, replication, blockSize, null, null);
				                              
				                                createdFile = copyFile;
				                                BufferedInputStream bis = new BufferedInputStream(in);
				                                StreamUtils.copy((InputStream)bis, fos);
				                                bis = null;
				                                fos.flush();
				                            }
				                            finally {
				                                try {
				                                    if (fos != null) {
				                                        fos.close();
				                                    }
				                                }
				                                catch (Throwable t) {
				                                    if (createdFile != null) {
				                                        try {
				                                            hdfs.delete(createdFile, false);
				                                        }
				                                        catch (Throwable t2) {}
				                                    }
				                                    throw t;
				                                }
				                                fos = null;
				                            }
				                        }
				                    });
		                    }        
		                  
		                    session.transfer(putFlowFile, HdfsWriter.REL_SUCCESS);
		                }
		                catch (Exception e) {
		                   HdfsWriter.this.getLogger().error("Failed to access HDFS due to {}", new Object[] { e });
		                   session.transfer(putFlowFile, HdfsWriter.REL_FAILURE);
		                    
		                }
                
                } // for
                
                
                return null;
            } // run
        }); // ugi
        
        
        
    }


}