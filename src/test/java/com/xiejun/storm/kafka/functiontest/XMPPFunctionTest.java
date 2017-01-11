package com.xiejun.storm.kafka.functiontest;

import java.io.IOException;

import org.jivesoftware.smack.ConnectionConfiguration.SecurityMode;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Message.Type;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.chat.Chat;
import org.jivesoftware.smack.chat.ChatManager;
import org.jivesoftware.smack.chat.ChatMessageListener;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;
import org.jivesoftware.smack.tcp.XMPPTCPConnectionConfiguration;

import com.xiejun.storm.kafka.function.XMPPFunction;

public class XMPPFunctionTest {
	
	public static void main(String[] args) throws SmackException, IOException, XMPPException, InterruptedException{
		
		XMPPTCPConnectionConfiguration config = XMPPTCPConnectionConfiguration.builder()
				.setHost("192.168.1.106").setServiceName("xiejun-machine")
				.setSecurityMode(SecurityMode.disabled).setDebuggerEnabled(true)
				.setSendPresence(false)
				.setUsernameAndPassword("storm1", "123456xj")
				.setResource("Spark").build();
		

		
		XMPPTCPConnection xmppConnection = new XMPPTCPConnection(config);
		try{
			xmppConnection.connect().login();

		}catch(Exception e){
			e.printStackTrace();
		}
		//xmppConnection.login();
		
		Chat chat = ChatManager.getInstanceFor(xmppConnection).createChat("xjtest1@xiejun-machine");
		chat.addMessageListener(new ChatMessageListener(){

			@Override
			public void processMessage(Chat arg0, Message arg1) {
				// TODO Auto-generated method stub
				System.out.println(arg1);
			}
			
		});
		
		//Message msg = new Message("xjtest1", Type.normal);
		//msg.setBody("dddddddddddddddddssssssssssssssssss");
		while(true){
			//xmppConnection.sendStanza(msg);
			chat.sendMessage("sddsdsd");
			Thread.sleep(5000);
		}

		
//		if(xmppConnection != null){
//			xmppConnection.login("storm1", "123456xj");
//		}else{
//			System.out.println("sssssssssssssssssssss");
//		}


		
	}

}
