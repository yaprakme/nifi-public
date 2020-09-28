# Nifi-CDC
Demonstrates custom CDC (change data capture) processors for Apache Nifi. Test verison is 1.11.4

### OracleLogMinerReader
This custom processor reads oracle bin logs via Oracle LogMiner packages 
.After the successful build and deploy drag OracleLogMinerReader from processors into your canvas 

![Alt text](/screens/oracle-logminer1.PNG?raw=true "")

- configure database connection pool to stand-by instance for non-cdb oracles  
- supports multi-tenant oracle, however can not connect to pluggable instance directly, database connection pool must be configured by root cdb instance.
- table names are comma-separated schema.table list
- commited and rollbacked transactions are supported
- compatible output with Nifi Mysql CDC (CaptureChangeMySQL)

#### Sample Output 

    {
      "type": "insert",
      "timestamp": "2020-07-06 14:14:06.0",
      "table_name": "DWH_NESTPAY_MERCHANT",
      "database": "DEMO",
      "row_id": "AAAR1FAAMAAADPRABn",
      "trx_code": null,
      "seq": "6595054",
      "columns": [
        {
          "name": "DATE",
          "value": "2020-07-06T14:14:06",
          "column_type": "date"
        },
        {
          "name": "BANKA_NAME",
          "value": "ISBANK",
          "column_type": "varchar"
        },
        {
          "name": "TRX_CODE",
          "value": "USD",
          "column_type": "varchar"
        },
        {
          "name": "SUM",
          "value": 1500,
          "column_type": "number"
        }
      ]
    }



# Build
    mvn clean package
    
# Deploy
    simply copy generated .ear file from target folder into $NIFI_HOME/lib folder
