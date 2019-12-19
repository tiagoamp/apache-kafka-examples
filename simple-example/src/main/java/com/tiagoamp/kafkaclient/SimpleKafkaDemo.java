package com.tiagoamp.kafkaclient;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleKafkaDemo {
	
	public static final String TOPIC_NAME = "simple-demo-test";
	

	public static void main(String[] args) throws Exception {
		
		// Producer 
		Producer<Long, String> producer = new SimpleProducer().getProducer();
		for (long i=1; i<=20; i++) {
			ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC_NAME, i, "data #" + i);
			Callback callback = (data, ex) -> {
				if (ex != null) throw new RuntimeException(ex);
				System.out.println("Sent message to topic ::"+ data.topic() +":: | timestamp " + data.timestamp());
			};
			producer.send(record, callback).get();  // blocking call for tests
		}
		System.out.println("All messages sent by producer");
		producer.close();
		
		//Consumer
		KafkaConsumer<Long,String> consumer = new SimpleConsumer().getConsumer();
		consumer.subscribe(Collections.singletonList(TOPIC_NAME));  // subscribes to a (list of) topics
		int noMessagesFound = 0; final byte MAX_NO_MESSAGE_FOUND_COUNT = 3;
		
		while (true) {
	    	ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));  // fetch data from the topics
	    	if (records.count() == 0) {
	            noMessagesFound++;
	            if (noMessagesFound > MAX_NO_MESSAGE_FOUND_COUNT) break;
	            else continue;
	        }
	    	records.forEach(
	    			record -> System.out.println( String.format("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()) )
	    		);	    	
	    }
	    consumer.close();
	    System.out.println("The end!");
	}
}


class SimpleProducer {
	
	public Producer<Long, String> getProducer() {
		Properties props = new Properties();  // producer configs
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);  // automatically retry
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<Long, String> producer = new KafkaProducer<>(props);
		return producer;
	}
	
}

class SimpleConsumer {
	
	public KafkaConsumer<Long, String> getConsumer() {
		Properties props = new Properties();  // consumer configs
		props.put("bootstrap.servers", "localhost:9092");
	    props.put("group.id", UUID.randomUUID().toString());
		props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("session.timeout.ms", "30000");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
	    return consumer;
	}
	
}
