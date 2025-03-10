package com.devs4j.kafka.assign_seek;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class consumer_assign_seek {
	private static final Logger log = LoggerFactory.getLogger(consumer_assign_seek.class);
	
	public static void main(String[] args) {
		Properties props = new Properties();
		
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "devs4j-group");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.offset.reset", "earliest");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		// Messages were written in Partition 0
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)){
			TopicPartition topicPartition = new TopicPartition("devs4j-topic", 0);
			// Assign receives an array of partitions to be assigned to the consumer
			consumer.assign(Arrays.asList(topicPartition));
			// Seek receives the partitions array and an offset for the consumer
			consumer.seek(topicPartition, 50);
			
			while (true) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.info("Offset = {}, Partition = {}, Key {}, Value = {}  ",
							consumerRecord.offset(), consumerRecord.partition(),
							consumerRecord.key(), consumerRecord.value());
				}
			}
		}
	}
}
