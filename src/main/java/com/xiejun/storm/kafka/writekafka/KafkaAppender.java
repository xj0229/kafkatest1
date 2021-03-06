package com.xiejun.storm.kafka.writekafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.xiejun.storm.kafka.utils.Formatter;
import com.xiejun.storm.kafka.utils.MessageFormatter;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class KafkaAppender extends AppenderBase<ILoggingEvent> {
	
	private String topic;
	
	private String zookeeperHost;
	
	private Producer<String,String> producer;
	
	private Formatter formatter;
	
	public String getTopic(){
		return topic;
	}
	
	public void setTopic(String topic){
		this.topic = topic;
	}

	public String getZookeeperHost() {
		return zookeeperHost;
	}

	public void setZookeeperHost(String zookeeperHost) {
		this.zookeeperHost = zookeeperHost;
	}

	public Formatter getFormatter() {
		return formatter;
	}

	public void setFormatter(Formatter formatter) {
		this.formatter = formatter;
	}

	@Override
	public void start(){
		if(this.formatter == null){
			this.formatter = new MessageFormatter();
		}
		
		super.start();
		
		Properties props = new Properties();
		
		props.put("zk.connect", this.zookeeperHost);
		
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		this.producer = new KafkaProducer<>(props);
	}
	
	@Override
	public void stop(){
		super.stop();
		
		this.producer.close();
		
	}
	
	@Override
	protected void append(ILoggingEvent arg0) {
		// TODO Auto-generated method stub
		String playload = this.formatter.format(arg0);
		
		ProducerRecord<String, String> data = new ProducerRecord<String, String>(this.topic, playload);
		
		this.producer.send(data);
		
	}

}
