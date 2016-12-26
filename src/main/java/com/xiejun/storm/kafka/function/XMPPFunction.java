package com.xiejun.storm.kafka.function;

import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.jivesoftware.smack.ConnectionConfiguration;
import org.jivesoftware.smack.XMPPConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiejun.storm.kafka.utils.MessageMapper;

public class XMPPFunction extends BaseFunction{
	private static final Logger LOG = LoggerFactory.getLogger(XMPPFunction.class);
	
	public static final String XMPP_TO = "storm.xmpp.to";
	
	public static final String XMPP_USER = "storm.xmpp.user";
	
	public static final String XMPP_PASSWORD = "storm.xmpp.password";
	
	public static final String XMPP_SERVER = "storm.xmpp.server";
	
	private XMPPConnection xmppConnection;
	
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
		
		ConnectionConfiguration config = new ConnectionConfiguration((String)conf.get(XMPP_SERVER));
		
		this.xmppConnection = new XMPPConnection(config);
		
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}
	

}
