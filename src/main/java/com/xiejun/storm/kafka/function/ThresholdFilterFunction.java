package com.xiejun.storm.kafka.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThresholdFilterFunction extends BaseFunction{
	
	private static final Logger LOG = LoggerFactory.getLogger(ThresholdFilterFunction.class);
	
	private static enum State{
		BELOW,
		ABOVE;
	}
	
	private State last = State.BELOW;
	
	private double threshold;
	
	public ThresholdFilterFunction(double threshold){
		this.threshold = threshold;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		double val = tuple.getDouble(0);
		
		State newState = val < this.threshold ? State.BELOW : State.ABOVE;
		
		boolean stateChange = this.last != newState;
		
		collector.emit(new Values(stateChange, threshold));
		
		this.last = newState;
		
		LOG.debug("State change? --> {}", stateChange);
	}

}
