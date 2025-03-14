package com.devs4j.kafka;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

// In this case, it was required to send a message right after the Spring application was launched
@SpringBootApplication
public class KafkaSpringApplication implements CommandLineRunner {
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	private static final Logger log = LoggerFactory.getLogger(KafkaSpringApplication.class);
	
	// Gets messages and puts them into the logs.
	@KafkaListener(topics="devs4j-topic", groupId="devs4j-group")
	public void listen(String message) {
		log.info("Message received {}", message);
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringApplication.class, args);
	}
	
	//@Override
	//public void run(String... args) throws Exception {
	//	kafkaTemplate.send("devs4j-topic", "Sample message");
	//}
	
	// With async callback --------------
	// Previous way (deprecated)
	/*
	@Override
	public void run(String... args) throws Exception {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic", "Sample message");
		future.addCallback(new KafkaSendCallback<String, String>);
		
		@Override
		public void onSuccess(SendResult<String, String> result){
			log.info("Message sent ", result.getRecordMetadata().offset(), );
		}
		
		@Override
		public void onFailure(Throwable ex){
			log.error("Error sending message ", ex);
		}
		
		@Override
		public void onFailure(KafkaProducerException ex){
			log.error("Error sending message ", ex);
		}
		
	}
	*/

	// New way
	@Override
	public void run(String... args) throws Exception {
		CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic", "Sample Message");

		future.whenComplete((result, ex) -> {
		    log.info("Message sent", result.getRecordMetadata().offset());
		});

	}

	
}
