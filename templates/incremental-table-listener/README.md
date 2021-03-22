# Nifi Template - Incremental table listener
Listens to sequential table inserts. Target table should have a sequence(incremental) column 

- checks target table every second (you can adjust this via Schedule tab)
- holds sequence number(offsetId) in Nifi state manager
- supports poll size (PartitionSize)
- starts from where you want (optioal StartIndexInput) otherwise from the end of table
- outputs json

![Alt text](inc1.PNG?raw=true "")

![Alt text](inc2.PNG?raw=true "")

# Test
   - import template
   - adjust data source pooling settings. 'databaseConnectionPoolName' denotes db service name for 'IncrementalTableListener' and 'Database Connection Pooling Service' is the same for 'ExecuteSQL'
   - update 'sqlSelectMaxId' to fetch latest sequence number for each iteration
   - update 'sqlToExecute' to listen to the desired table ('between :prm1 and :prm2' block should not be changed) 

# Build
    mvn clean package
    
# Deploy
    simply import template file into suitable Apache Nifi 

