package com.xiejun.storm.kafka.sourcedata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RogueApplication {
	private static final Logger LOG = LoggerFactory.getLogger(RogueApplication.class);

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		int slowCount = 6;
		
		int fastCount = 15;
		
		//slow state
		for(int i = 0; i < slowCount; i++){
			LOG.warn("this is a warning (slow state).");
			Thread.sleep(5000);
		}
		
		//enter rapid state
		for(int i = 0; i < fastCount; i++){
			LOG.warn("this is a warning (rapid state).");
			Thread.sleep(1000);
		}
		
		//return to slow state
		for(int i = 0; i < slowCount; i++){
			LOG.warn("this is a warning (slow state).");
			Thread.sleep(5000);
		}

	}

}
