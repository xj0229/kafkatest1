package com.xiejun.storm.kafka.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

import com.xiejun.storm.kafka.filter.BooleanFilter;
import com.xiejun.storm.kafka.function.JsonProjectFunction;
import com.xiejun.storm.kafka.function.MovingAverageFunction;
import com.xiejun.storm.kafka.function.ThresholdFilterFunction;
import com.xiejun.storm.kafka.function.XMPPFunction;
import com.xiejun.storm.kafka.utils.EWMA;
import com.xiejun.storm.kafka.utils.EWMA.Time;
import com.xiejun.storm.kafka.utils.NotifyMessageMapper;

public class LogAnalysisTopology {
	
	public static StormTopology buildTopology(){
		TridentTopology topology = new TridentTopology();
		
		//BrokerHosts zk = new ZkHosts("localhost");
		
		BrokerHosts zk = new ZkHosts("192.168.1.114");
		
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "ktest");
		
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		spoutConf.startOffsetTime = -1;
		
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		
		Stream spoutStream = topology.newStream("kafka-stream", spout);
		
		Fields jsonFields = new Fields("level", "timestamp", "message", "logger");
		
		Stream parsedStream = spoutStream.each(new Fields("str"), new JsonProjectFunction(jsonFields), jsonFields);
		
		parsedStream = parsedStream.project(jsonFields);
		
		EWMA ewma = new EWMA().sliding(1.0, Time.MINUTES).withAlpha(EWMA.ONE_MINUTE_ALPHA);
		
		Stream averageStream = parsedStream.each(new Fields("timestamp"), new MovingAverageFunction(ewma, Time.MINUTES), new Fields("average"));
		
		ThresholdFilterFunction tff = new ThresholdFilterFunction(50D);
		
		Stream thresholdStream = averageStream.each(new Fields("average"), tff, new Fields("change", "threshold"));
		
		Stream filteredStream = thresholdStream.each(new Fields("change"), new BooleanFilter());
		
		filteredStream.each(filteredStream.getOutputFields(), new XMPPFunction(new NotifyMessageMapper()), new Fields());
		
		return topology.build();
		
	}
	
	//do not have log4j appender
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException{
		Config config = new Config();
		
		config.put(XMPPFunction.XMPP_USER, "storm1");
		
		config.put(XMPPFunction.XMPP_PASSWORD, "123456xj");
		
		config.put(XMPPFunction.XMPP_SERVER, "xiejun-machine");
		
		config.put(XMPPFunction.XMPP_TO, "xjtest1");
		
		config.setMaxSpoutPending(5);
		
		if(args.length == 0){
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("log-analysis", config, buildTopology());
		}else{
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config, buildTopology());
		}
		
	}
	

}
