
package org.apache.nifi.processors.cdc;

import redis.embedded.RedisServer;


public class RunEmbeddedRedis {

	//https://github.com/kstyrc/embedded-redis
    //https://github.com/microsoftarchive/redis/releases
    
    public static void main(String[] args) {
    	try {
    		RedisServer redisServer = new RedisServer(6379);
			redisServer.start();

			System.out.println("Press enter to stop the Redis server...");
	        System.in.read();
	        
	        if (redisServer != null) {
	            redisServer.stop();
	            System.out.println("Redis stopped. Close the application");
	        }
	            
		} catch (Exception e) {
			e.printStackTrace();
		}
        
	}
    
    
}
