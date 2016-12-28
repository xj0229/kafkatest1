package com.xiejun.storm.kafka.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;

public interface Formatter {
	
	String format(ILoggingEvent event);

}