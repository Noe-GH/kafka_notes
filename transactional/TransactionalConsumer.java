package com.devs4j.kafka.transactional;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.devs4j.kafka.consumer.Devs4jConsumer;

public class TransactionalConsumer {

	private static final Logger log = LoggerFactory.getLogger(TransactionalConsumer.class);
	public static void main(String[] args) {
		Properties props = new Properties();
		
		props.setProperty("bootstrap.servers","localhost:9092");
		props.setProperty("group.id","devs4j-group");
		props.setProperty("enable.auto.commit","true");
		props.setProperty("auto.commit.interval.ms","1000");
		props.setProperty("key.deserializer",
		"org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer",
		"org.apache.kafka.common.serialization.StringDeserializer");
		
		// For transactions, isolation.level should be set to read_commited (reads only commited messages)
		// This defaults to read_uncommited (reading all messages independently if they were commited or not)
		props.setProperty("isolation.level","read_committed");
		
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)){
			consumer.subscribe(Arrays.asList("devs4j-topic"));
			while(true) {

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
