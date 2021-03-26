# Nifi Template - CaptureChangeSqlServer To Kafka
Captures database events(insert/update/delete) and put them into Kafka

- checks target table every second (you can adjust this via Schedule tab)
- holds sequence number in Nifi state manager
- supports poll size (MaxPollSize)
- starts from where you want (optioal StartDate) 
- outputs json

![Alt text](ms-cdc1.PNG?raw=true "")

![Alt text](ms-cdc2.PNG?raw=true "")

#### Enable CDC

-- enable cdc on database

EXEC sys.sp_cdc_enable_db; 

-- create capture for specific table such as dbo.Person

EXECUTE sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Person',  @role_name = NULL;   

-- check created capture

EXECUTE sys.sp_cdc_help_change_data_capture    
    @source_schema = N'dbo',   
    @source_name = N'Person'

	
[for more info aboout sql server cdc click](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server?view=sql-server-ver15)
 

# Test
   - configure a ExecuteScript processor with groovy script to test only CDC or import template
   - adjust data source pooling settings. 'ConnectionName' denotes db service name
   
   
#### Sample Sql Server CDC Output 

	{
	  "columns": [
	    "Id",
	    "Name",
	    "Surname",
	    "Age",
	    "Sum",
	    "Create_Date"
	  ],
	  "types": [
	    "int",
	    "nchar",
	    "nchar",
	    "int",
	    "float",
	    "datetime"
	  ],
	  "keyColumns": [
	    "Id"
	  ],
	  "data": [
	    "23",
	    "ddd       ",
	    "fff       ",
	    "45",
	    "12300.0",
	    "26-03-2021 13:51:38.100"
	  ],
	  "before": [
	    "23",
	    "ddd       ",
	    "fff       ",
	    "23",
	    "12300.0",
	    "26-03-2021 13:51:38.100"
	  ],
	  "operation": "update",
	  "trxDate": "2021-03-26 13:51:50.193",
	  "schema": "dbo",
	  "table": "Person",
	  "seq": "0x0000085c00007b000003"
	}


	{
	  "columns": [
	    "Id",
	    "Name",
	    "Surname",
	    "Age",
	    "Sum",
	    "Create_Date"
	  ],
	  "types": [
	    "int",
	    "nchar",
	    "nchar",
	    "int",
	    "float",
	    "datetime"
	  ],
	  "keyColumns": [
	    "Id"
	  ],
	  "data": [
	    "12",
	    "George    ",
	    "Small     ",
	    "36",
	    "1200.0",
	    "26-03-2021 12:21:21.533"
	  ],
	  "before": [
	    "12",
	    "George    ",
	    "Slim      ",
	    "34",
	    "1200.0",
	    "26-03-2021 12:21:21.533"
	  ],
	  "operation": "update",
	  "trxDate": "2021-03-26 12:22:03.58",
	  "schema": "dbo",
	  "table": "Person",
	  "seq": "0x0000085c000059400003"
	}


	{
	  "columns": [
	    "Id",
	    "Name",
	    "Surname",
	    "Age",
	    "Sum",
	    "Create_Date"
	  ],
	  "types": [
	    "int",
	    "nchar",
	    "nchar",
	    "int",
	    "float",
	    "datetime"
	  ],
	  "keyColumns": [
	    "Id"
	  ],
	  "data": [
	    "12",
	    "George    ",
	    "Small     ",
	    "36",
	    "1200.0",
	    "26-03-2021 12:21:21.533"
	  ],
	  "before": null,
	  "operation": "delete",
	  "trxDate": "2021-03-26 12:22:15.52",
	  "schema": "dbo",
	  "table": "Person",
	  "seq": "0x0000085c000059600005"
	}






