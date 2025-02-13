package com.devs4j.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Devs4jProducer {
	
	public static final Logger log = LoggerFactory.getLogger(Devs4jProducer.class);
	public static void main(String[] args) {
		Properties props=new Properties();
		// Kafka broker to connect to
		props.put("bootstrap.servers","localhost:9092");
		//props.put("bootstrap.servers","172.31.101.179:9092");
		// If 0, doesn't matter if messages arrive or not
		// If 1, 1 acknowledge is necessary
		// If all, all brokers have to acknowledge the message
		props.put("acks","1");
		
		props.put("key.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		
		// try is used this way so Java is able to find the close method automatically without using finally and producer.close()
		try(Producer<String, String>producer=new KafkaProducer<>(props);) {
			// Asynchronous call
			//producer.send(new ProducerRecord<String, String>("devs4j-topic", "devs4j-key", "devs4j-key"));
			
			for(int i=0;i<10000;i++) {
				// Asynchronous call (not waiting for messages to be delivered in order to send the rest of them)
				//producer.send(new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i), "devs4j-key"));
				
				// Synchronous call
				producer.send(new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i), "devs4j-key")).get();
			}
			
			// Method used so that it sends everything that is pending to be sent, and messages are delivered.
			producer.flush();
			// For asynchronous calls, it was almost instantaneous
			
			// For synchronous calls, it was slower

		// Catch was added when testing for Synchronous call. It was necessary.
		// It is also necessary to omit it for Asynchronous calls
		} catch(InterruptedException | ExecutionException e) {
			log.error("Message producer interrupted ", e);
		}

	}
}
