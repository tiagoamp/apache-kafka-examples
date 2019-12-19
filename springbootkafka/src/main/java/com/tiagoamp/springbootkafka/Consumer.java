package com.tiagoamp.springbootkafka;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
	
	private final Logger logger = LoggerFactory.getLogger(Producer.class);

    @KafkaListener(topics = "spring-demo", groupId = "group_id")
    public void consume(String message) throws IOException {
    	logger.info("<=== Consumed message = " + message);
    }

}
