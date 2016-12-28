package com.xiejun.storm.kafka.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;

public class MessageFormatter implements Formatter{

	public String format(ILoggingEvent event) {
		// TODO Auto-generated method stub
		return event.getFormattedMessage();
	}

}