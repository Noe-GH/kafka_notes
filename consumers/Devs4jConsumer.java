package com.devs4j.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Devs4jConsumer {
	private static final Logger log = LoggerFactory.getLogger(Devs4jConsumer.class);
	public static void main(String[] args) {
		Properties props = new Properties();
		
		props.setProperty("bootstrap.servers","localhost:9092");
		
		// ID for consumer group
		props.setProperty("group.id","devs4j-group");
		
		// Auto-commit to offsets
		props.setProperty("enable.auto.commit","true");
		props.setProperty("auto.commit.interval.ms","1000");
		props.setProperty("key.deserializer",
		"org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer",
		"org.apache.kafka.common.serialization.StringDeserializer");
		
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)){
			// Subscribe to one or multiple topics
			consumer.subscribe(Arrays.asList("devs4j-topic"));
			// Infinite loop so it is executed all the time
			while(true) {
				// Getting messages from Kafka in 100 ms.
				// Messages are stored in the variable.
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				
				for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.info("Offset = {}, Partition = {}, Key = {}, Value = {}", consumerRecord.offset(),
							consumerRecord.partition(),
							consumerRecord.key(),
							consumerRecord.value());
				}
			}
			
		}
		
	}
}
