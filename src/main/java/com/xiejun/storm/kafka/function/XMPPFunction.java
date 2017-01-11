package com.xiejun.storm.kafka.function;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.jivesoftware.smack.ConnectionConfiguration.SecurityMode;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.SmackException.NotConnectedException;
import org.jivesoftware.smack.chat.Chat;
import org.jivesoftware.smack.chat.ChatManager;
import org.jivesoftware.smack.chat.ChatMessageListener;
import org.jivesoftware.smack.XMPPException;
import org.jivesoftware.smack.packet.Message;
import org.jivesoftware.smack.packet.Message.Type;
import org.jivesoftware.smack.tcp.XMPPTCPConnection;
import org.jivesoftware.smack.tcp.XMPPTCPConnectionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiejun.storm.kafka.utils.MessageMapper;

public class XMPPFunction extends BaseFunction{
	private static final Logger LOG = LoggerFactory.getLogger(XMPPFunction.class);
	
	public static final String XMPP_TO = "storm.xmpp.to";
	
	public static final String XMPP_USER = "storm.xmpp.user";
	
	public static final String XMPP_PASSWORD = "storm.xmpp.password";
	
	public static final String XMPP_SERVER = "storm.xmpp.server";
	
	private XMPPTCPConnection xmppConnection;
	
	private String to;
	
	private MessageMapper mapper;
	
	private Chat chat;
	
	public XMPPFunction(MessageMapper mapper){
		this.mapper = mapper;
	}
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		LOG.debug("Prepare: {}", conf);
		
		super.prepare(conf, context);
		
		this.to = (String)conf.get(XMPP_TO);
		
		XMPPTCPConnectionConfiguration config = XMPPTCPConnectionConfiguration.builder()
				.setHost("192.168.1.106").setServiceName((String) conf.get(XMPP_SERVER))
				.setSecurityMode(SecurityMode.disabled)
				.setDebuggerEnabled(true)
				.build();
		//ConnectionConfiguration config = new XMPPTCPConnectionConfiguration(null);
		
		this.xmppConnection = new XMPPTCPConnection(config);
		
		try{
			this.xmppConnection.connect();
			this.xmppConnection.login((String)conf.get(XMPP_USER), (String)conf.get(XMPP_PASSWORD));
			
			//20170111---------------------
			this.chat = ChatManager.getInstanceFor(this.xmppConnection).createChat("xjtest1@xiejun-machine");
			this.chat.addMessageListener(new ChatMessageListener(){

				@Override
				public void processMessage(Chat arg0, Message arg1) {
					// TODO Auto-generated method stub
					System.out.println(arg1);
				}
				
			});
			//20170111---------------------
			
		}catch(SmackException | IOException | XMPPException e){//this only fit to JRE 1.7
			LOG.warn("Error initializing XMPP Channel", e);
		}
		
	}
	
//Override Annotation is only fit to JRE 1.7?
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		Message msg = new Message();//this.to, Type.normal);
		msg.setBody(this.mapper.toMessageBody(tuple));
		
/*		try {
			this.xmppConnection.sendStanza(msg);//sendPacket(msg);
		} catch (NotConnectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		//20170111---------------------
		try {
			this.chat.sendMessage(msg);
		} catch (NotConnectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//20170111---------------------
		
	}

}
