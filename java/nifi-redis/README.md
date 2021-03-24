# Nifi-Redis
Demonstrates custom Redis based processors for Apache Nifi. Test verison is 1.11.4

### TimeWindow
This custom processor makes aggregation windows on a data stream. After the successful build and deploy drag TimeWindow from processors into your canvas 

- Thumbling and Hopping window 
- sum, count, avg are supported
- calculations are fulfilled in Redis through lua scripting
- uses system time

![Alt text](timewindow1.PNG?raw=true "")
#### Sample Scenario 
Start RunEmbeddedRedis to simulate redis if you dont have real one.
Let's configure a 300 sec window with 60 sec hop length. grouping field:card_no, aggregation_type:sum, aggregation_field:trx_amount
- starts at **T** 
- **T + 1** incoming event:
{"customer_no":"1","card_no":"cc11","trx_amount":100}
- **T + 40** incoming event:
{"customer_no":"2","card_no":"cc21","trx_amount":250}
- **T + 60** encounters a checkpoint. aggregated and produced events:
{"card_no":"cc11", "trx_amount_sum":100}
{"card_no":"cc21", "trx_amount_sum":250}
- **T + 70** incoming event:
{"customer_no":"1","card_no":"cc11","trx_amount":150}
- **T + 100** incoming event:
{"customer_no":"2","card_no":"cc21","trx_amount":300}
- **T + 120** encounters a checkpoint. aggregated and produced events:
{"card_no":"cc11", "trx_amount_sum":250}
{"card_no":"cc21", "trx_amount_sum":550}
- no event for a while,  **T + 180**, **T+240** and **T+ 300** encounters checkpoint. aggregated and produced events:
{"card_no":"cc11", "trx_amount_sum":250}
{"card_no":"cc21", "trx_amount_sum":550}
- **T + 305** incoming event:
{"customer_no":"1","card_no":"cc11","trx_amount":200}
- **T + 345** incoming event:
{"customer_no":"2","card_no":"cc21","trx_amount":350}
- **T + 360** encounters a checkpoint. aggregated and produced events:
{"card_no":"cc11", "trx_amount_sum":350}
{"card_no":"cc12", "trx_amount_sum":650}
(first 2 events are no more in aggregation at this point)


# Build
    mvn clean package
    
# Deploy
    simply copy generated .nar file from target folder into $NIFI_HOME/lib folder

