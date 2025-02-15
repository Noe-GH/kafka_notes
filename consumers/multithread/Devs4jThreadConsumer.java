package multithread;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.errors.WakeupException;
import com.devs4j.kafka.consumer.Devs4jConsumer;


public class Devs4jThreadConsumer extends Thread {
	
	// consumer variable used as a parameter for the constructor.
	private final KafkaConsumer<String, String> consumer;
	
	// It is initialized as false, because the consumer is not starting closed.
	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	private static final Logger log = LoggerFactory.getLogger(Devs4jConsumer.class);
	
	public Devs4jThreadConsumer(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}
	// 1:50
	
	@Override
	public void run() {
		//
		consumer.subscribe(Arrays.asList("devs4j-topic"));
		try {
			while(!closed.get()) {
			  
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				
				for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					log.info("Offset = {}, Partition = {}, Key = {}, Value = {}", consumerRecord.offset(),
							consumerRecord.partition(),
							consumerRecord.key(),
							consumerRecord.value());
				}
			  
			}
		}catch(WakeupException e) {
			if(!closed.get()) {
				throw e;
			}
		}finally {
			consumer.close();
		}

	}
	
	public void shutdown() {
		closed.set(true);
		// Thread safe method. Useful to abort a long poll.
		consumer.wakeup();
	}

}
