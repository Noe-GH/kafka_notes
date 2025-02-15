package multithread;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Devs4jMultithreadConsumer {
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
		
		// Useful for creating a thread pool.
		// Set for 5 threads
		ExecutorService executor = Executors.newFixedThreadPool(5);
		
		// It's iterating 5 times, as there are 5 threads and 5 partitions
		for(int i=0; i<5; i++) {
			// Class created in a previous step
			Devs4jThreadConsumer consumer = new Devs4jThreadConsumer(new KafkaConsumer<>(props));
			// Thread pool executing the consumer
			executor.execute(consumer);
		}
		while(!executor.isTerminated());
	}

}

// Output
//1152 [pool-1-thread-4] INFO com.devs4j.kafka.consumer.Devs4jConsumer - Offset = 0, Partition = 3, Key = null, Value = hello
//23845 [pool-1-thread-2] INFO com.devs4j.kafka.consumer.Devs4jConsumer - Offset = 1, Partition = 1, Key = null, Value = 1
//23845 [pool-1-thread-2] INFO com.devs4j.kafka.consumer.Devs4jConsumer - Offset = 2, Partition = 1, Key = null, Value = 2
//23846 [pool-1-thread-2] INFO com.devs4j.kafka.consumer.Devs4jConsumer - Offset = 3, Partition = 1, Key = null, Value = 3
//23846 [pool-1-thread-2] INFO com.devs4j.kafka.consumer.Devs4jConsumer - Offset = 4, Partition = 1, Key = null, Value = 4
//25971 [pool-1-thread-1] INFO com.devs4j.kafka.consumer.Devs4jConsumer - Offset = 0, Partition = 0, Key = null, Value = 5

