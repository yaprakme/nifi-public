# Nifi Hadoop Customs
Demonstrates a HdfsWriter for Apache Nifi. supports insert/update/delete

- supports path and file operations
- filename attribute indicates file name in hadoop
- operation attribute determines insert/upsert or delete

![Alt text](hdfs1.PNG?raw=true "")

![Alt text](hdfs2.PNG?raw=true "")

# Test
    import template file into Nifi or run junit tests

# Build
    mvn clean package
    
# Deploy
    simply copy generated .nar file from target folder into $NIFI_HOME/lib folder

