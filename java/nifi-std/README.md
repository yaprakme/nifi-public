### DefaultCryptographer
   - encrypt, decrypt and hash incoming flow file content according to iv_spec and field descriptor
![Alt text](crypto1.PNG?raw=true "")

### GetTCP
   - since there is no standard request/response tcp processor in Nifi , this processor sends piece of login information to receive stream of data from specified ip:port
   - in order to run junit test, you should start MockStreamServer first  
![Alt text](tcp1.PNG?raw=true "")

# Build
    mvn install
    
# Deploy
   - simply copy generated .ear file from target folder into $NIFI_HOME/lib folder
   - import template files into Nifi to see live examples  