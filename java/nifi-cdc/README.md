# Nifi Oracle CDC 
Demonstrates custom CDC (change data capture) processors for Apache Nifi.

### OracleLogMinerReader (CaptureChangeOracle)
This custom processor reads oracle bin logs via Oracle LogMiner packages 
.After the successful build and deploy drag OracleLogMinerReader from processors into your canvas 

![Alt text](logminer1.PNG?raw=true "")

![Alt text](logminer2.PNG?raw=true "")

- table names are comma-separated schema.table list
- supports alter statements  by 'session' prefixed dynamic properties
- adjustable poll interval via 'Run schedule' (default 1 sn)
- commited and rollbacked transactions are supported
- compatible output with Nifi Mysql CDC (CaptureChangeMySQL)

#### Enable logminer

1.Invoke SQL*Plus and connect as a user with SYSDBA privileges.
2.SHUTDOWN IMMEDIATE 
3.STARTUP MOUNT
4.ALTER DATABASE ARCHIVELOG;
5.ALTER DATABASE OPEN;
6.ARCHIVE LOG LIST

#### Sample Output 

    {
      "type": "insert",
      "timestamp": "2020-07-06 14:14:06.0",
      "table_name": "MERCHANT",
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
          "value": "IBANK",
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

	{
	  "type": "update",
	  "timestamp": "2021-03-25 17:42:18.0",
	  "table_name": "PERSON",
	  "database": "LOGMINER",
	  "row_id": "AAASTJAABAAAZoWAAB",
	  "trx_code": null,
	  "seq": "4741420",
	  "columns": [
		{
		  "name": "ID",
		  "value": 0,
		  "column_type": "number",
		  "last_value": 0
		},
		{
		  "name": "NAME",
		  "value": "name changed",
		  "column_type": "varchar",
		  "last_value": "name"
		},
		{
		  "name": "BIRTH_DATE",
		  "value": "2021-03-25T17:42:18",
		  "column_type": "date",
		  "last_value": "2021-03-25T17:42:18"
		},
		{
		  "name": "SURNAME",
		  "value": "surname",
		  "column_type": "varchar",
		  "last_value": "surname"
		},
		{
		  "name": "ROWID",
		  "value": "AAASTJAABAAAZoWAAB",
		  "column_type": "varchar",
		  "last_value": "AAASTJAABAAAZoWAAB"
		}
	  ]
	}

	{
	  "type": "delete",
	  "timestamp": "2021-03-25 17:42:20.0",
	  "table_name": "PERSON",
	  "database": "LOGMINER",
	  "row_id": "AAASTJAABAAAZoWAAB",
	  "trx_code": null,
	  "seq": "4741425",
	  "columns": [
		{
		  "name": "ID",
		  "value": 0,
		  "column_type": "number"
		},
		{
		  "name": "NAME",
		  "value": "name changed",
		  "column_type": "varchar"
		},
		{
		  "name": "BIRTH_DATE",
		  "value": "2021-03-25T17:42:18",
		  "column_type": "date"
		},
		{
		  "name": "SURNAME",
		  "value": "surname",
		  "column_type": "varchar"
		},
		{
		  "name": "ROWID",
		  "value": "AAASTJAABAAAZoWAAB",
		  "column_type": "varchar"
		}
	  ]
	}
	
# Build
    mvn clean package
    
# Deploy
    simply copy generated .nar file from target folder into $NIFI_HOME/lib folder
