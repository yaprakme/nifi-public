# Nifi Quickfix/J Initiator
Demonstrates quickfix/j initiator processor for Apache Nifi.

- holds message sequence numbers in Nifi state manager
- supports dynamic properties
- outputs json

![Alt text](fix1.PNG?raw=true "")

![Alt text](fix2.PNG?raw=true "")

# Test
    Start Acceptor in test folder to simulate Acceptor and then junit test

# Build
    mvn clean package
    
# Deploy
    simply copy generated .ear file from target folder into $NIFI_HOME/lib folder

