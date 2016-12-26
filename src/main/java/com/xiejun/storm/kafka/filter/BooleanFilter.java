package com.xiejun.storm.kafka.filter;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class BooleanFilter extends BaseFilter{

	@Override
	public boolean isKeep(TridentTuple tuple) {
		// TODO Auto-generated method stub
		return tuple.getBoolean(0);
	}

}
