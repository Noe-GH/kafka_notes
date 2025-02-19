package com.devs4j.kafka.transactional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.devs4j.kafka.producers.Devs4jProducer;

public class TransactionalProducer {
	
	public static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		Properties props=new Properties();

		props.put("bootstrap.servers","localhost:9092");
		
		// For transactions, acks has to be all
		props.put("acks","all");
		// For transactions, transactional.id should be set
		props.put("transactional.id", "devs4j-producer-id");
		
		props.put("key.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "15");


		try(Producer<String, String>producer=new KafkaProducer<>(props);) {
			
			try {
				producer.initTransactions();
				producer.beginTransaction();
				
				for(int i=0;i<100000;i++) {
					producer.send(new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i), "devs4j-key"));
					
					// Error simulation
					if(i==50000) {
						throw new Exception("Unexpected Exception");
					}

				}
				
				// If everything was fine, we commit transactions
				producer.commitTransaction();
				
				producer.flush();

			} catch(Exception e){
				log.error("Error ", e);
				
				// If something goes wrong, transaction is aborted
				// With the error simulation, there were no messages consumed by the transactional consumer
				// A non-transactional consumer consumes the messages produced before the error
				producer.abortTransaction();
			}
			
		}
		
		log.info("Processing time = {} ms", (System.currentTimeMillis() - startTime));

	}
}
