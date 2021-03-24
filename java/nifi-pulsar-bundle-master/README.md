# Nifi Pulsar
Demonstrates pulsar processors for Apache Nifi. Based on [openconnectors](https://github.com/openconnectors/nifi-pulsar-bundle)

### PublishPulsar
- supports batch send without demarcation

### ConsumePulsar
- supports batch receive and back pressure

### ReaderPulsar
- implements Pulsar reader API
- listens to the topic without subscription
- persists last message id in Nifi satate manager

![Alt text](pulsar1.PNG?raw=true "")

# Test
    - import tamplate  
    - Start a pulsar server
    - adjust StandardPulsarClientService and enable it
# Build
    mvn clean package
    
# Deploy
    simply copy generated .nar file from target folder into $NIFI_HOME/lib folder

