package org.apache.nifi.processors.socket;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class MockStreamServer {
       
	static ServerSocketChannel serverSocket = null;
	static SocketChannel channel = null;
	
	public static void main(String[] args) throws Exception {
		
		serverSocket = ServerSocketChannel.open();
		serverSocket.socket().bind(new InetSocketAddress(9000));
		channel = serverSocket.accept();
		System.out.println("Connection Established");
	    
		String response = checkLogon();
		
		if  ( response.equals("LOGON")) {  
			
			System.out.println("logged in");
			
			int index = 1;
		    while(true) {
			  
		    	StringBuilder sb = new StringBuilder();	
		    	sb.append("068" + FF() + "STOCK"+ (index++) + FF() + "8.43" + FF() + "\r\n");
		    	sb.append("069" + FF() + "STOCK"+ (index++) + FF() + "8.43" + FF() + "\r\n");
		    	
		    	write(sb.toString().getBytes());
		       
		    	Thread.currentThread().sleep(1000);
		    }
			
			
		}
	
	}
	
	
   static String FF() {
	   return Character.toString((char)255);
   }
  
	
   static void write(byte[] data) throws Exception {
	   ByteBuffer buffer = ByteBuffer.wrap(data);
	   channel.write(buffer); 
   }
	
   
   static String checkLogon() throws Exception {
	   ByteBuffer buffer = ByteBuffer.allocate(1024);
	   int byteCount = channel.read(buffer);
	   if (byteCount > 0) {
		   return new String(buffer.array()).trim();
	   }
	   return null;
   }
   
	
}
