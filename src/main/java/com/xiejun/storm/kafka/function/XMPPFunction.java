package com.xiejun.storm.kafka.function;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.jivesoftware.smack.SmackException;
import org.jivesoftware.smack.SmackException.NotConnectedException;
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
	
	public XMPPFunction(MessageMapper mapper){
		this.mapper = mapper;
	}
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		LOG.debug("Prepare: {}", conf);
		
		super.prepare(conf, context);
		
		this.to = (String)conf.get(XMPP_TO);
		
		XMPPTCPConnectionConfiguration config = XMPPTCPConnectionConfiguration.builder()
				.setHost((String)conf.get(XMPP_SERVER)).build();
		//ConnectionConfiguration config = new XMPPTCPConnectionConfiguration(null);
		
		this.xmppConnection = new XMPPTCPConnection(config);
		
		try{
			this.xmppConnection.connect();
			this.xmppConnection.login((String)conf.get(XMPP_USER), (String)conf.get(XMPP_PASSWORD));
			
		}catch(SmackException | IOException | XMPPException e){
			LOG.warn("Error initializing XMPP Channel", e);
		}
		
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		Message msg = new Message(this.to, Type.normal);
		msg.setBody(this.mapper.toMessageBody(tuple));
		try {
			this.xmppConnection.sendPacket(msg);
		} catch (NotConnectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

}
