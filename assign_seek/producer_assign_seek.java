package com.devs4j.kafka.assign_seek;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class producer_assign_seek {
	public static final Logger log = LoggerFactory.getLogger(producer_assign_seek.class);
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "10");
		
		try(Producer<String, String> producer = new KafkaProducer<>(props);){
			for(int i=0; i<100; i++) {
				producer.send(new ProducerRecord<String, String>("devs4j-topic", "devs4j-key", String.valueOf(i)));
			}
			producer.flush();
		}
		
		log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));
	}


}
