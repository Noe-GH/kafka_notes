package com.devs4j.kafka.callbacks;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.devs4j.kafka.producers.Devs4jProducer;

public class Devs4jCallbackProducer {

	public static final Logger log = LoggerFactory.getLogger(Devs4jCallbackProducer.class);
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		Properties props=new Properties();

		props.put("bootstrap.servers","localhost:9092");
		props.put("acks","1");
		props.put("key.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "15");
		
		try(Producer<String, String>producer=new KafkaProducer<>(props);) {
			
			for(int i=0;i<10000;i++) {
				
				// For callback added new Callback()
				// Callbacks allow for executing actions when a message is delivered
				producer.send(new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i), "devs4j-key"),
						new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						
						// Actions if there is an exception
						if(exception != null) {
							log.info("There was an error {}", exception.getMessage());
						}
						log.info("Offset = {}, Partition = {}, Topic = {}",
								metadata.offset(), metadata.partition(), metadata.topic());
					}
				}
						
						);
				
			}
			
			producer.flush();

		}
		
		log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));

	}

}
