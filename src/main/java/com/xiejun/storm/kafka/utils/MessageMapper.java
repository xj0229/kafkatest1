package com.xiejun.storm.kafka.utils;

import java.io.Serializable;

import org.apache.storm.trident.tuple.TridentTuple;

public interface MessageMapper extends Serializable{
	public String toMessageBody(TridentTuple tuple);

}
