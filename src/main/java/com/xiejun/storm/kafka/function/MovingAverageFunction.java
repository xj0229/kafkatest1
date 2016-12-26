package com.xiejun.storm.kafka.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiejun.storm.kafka.utils.EWMA;
import com.xiejun.storm.kafka.utils.EWMA.Time;

public class MovingAverageFunction extends BaseFunction{
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseFunction.class);
	
	private EWMA ewma;
	
	private Time emitRatePer;
	
	public MovingAverageFunction(EWMA ewma, Time emitRatePer){
		this.ewma = ewma;
		this.emitRatePer = emitRatePer;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		this.ewma.mark(tuple.getLong(0));
		LOG.debug("Rate: {}", this.ewma.getAverageRatePer(this.emitRatePer));
		collector.emit(new Values(this.ewma.getAverageRatePer(this.emitRatePer)));
	}

}
