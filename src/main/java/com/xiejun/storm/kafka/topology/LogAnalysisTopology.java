package com.xiejun.storm.kafka.topology;

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

public class LogAnalysisTopology {
	
	public static StormTopology buildTopology(){
		TridentTopology topology = new TridentTopology();
		
		BrokerHosts zk = new ZkHosts("localhost");
		
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "log-analysis");
		
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		
		spoutConf.startOffsetTime = -1;
		
		Stream spoutStream = topology.newStream("kafka-stream", (IRichSpout) spoutConf);
		
		
		
	}
	

}
