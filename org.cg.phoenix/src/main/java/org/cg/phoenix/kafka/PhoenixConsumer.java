package org.cg.phoenix.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.log4j.Logger;
import org.cg.phoenix.util.AvroDesrializer;
import org.cg.phoenix.util.PhoenixWriter;


public abstract class PhoenixConsumer<V>{
	private static Logger logger = Logger.getLogger(PhoenixConsumer.class);

	private final ConsumerConnector consumer;
	private final String topic;
	private  ExecutorService executor;
	public PhoenixWriter<V> pWriter;
	private int CommitBatchSize = 100;
	private Class<V> eventType;
	
	
	/*
	 * 
	 *  PhoenixWriter needs to be initialized when 
	 *  implementing this method, eg:
	 *  PhoenixWriter = new PhoenixEventWriter(address, tableName);
	 * 
	 */

	public PhoenixConsumer(Class<V> eventType, String a_zookeeper, String a_groupId, String a_topic, String address, String tableName, int CommitBatchSize) {
		initialPhoenixWriter( address, tableName);
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
				createConsumerConfig(a_zookeeper, a_groupId));
		this.topic = a_topic;
		this.CommitBatchSize = CommitBatchSize;
		this.eventType = eventType;
		if(this.pWriter == null){
			throw new NullPointerException ("Did you forget to implement or initialize PhoenixWriter?");
		}
		
	}
	
	public abstract void initialPhoenixWriter(String address, String tableName);
	


	public void shutdown() {


		pWriter.commit();
		pWriter.close();

		if (consumer != null) consumer.shutdown();
		if (executor != null) executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}



	}


	public class ConsumerThread implements Runnable {
		private KafkaStream<byte[], byte[]> m_stream;
		private int m_threadNumber;
		private AvroDesrializer<V> aDesrializer;
		
		
		public ConsumerThread(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber) {
			m_threadNumber = a_threadNumber;
			m_stream = a_stream;
			aDesrializer = new AvroDesrializer<V>(eventType);
		}

		public void run() {
			ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
			int counter = 0;
			long startTime = 0;
			while (it.hasNext()){

				V event = aDesrializer.byteToEvent(it.next().message());
				//logger.info("Thread " + m_threadNumber + ": " + event.toString());

				pWriter.upsertEvents(event);
				counter++;
				if(counter%CommitBatchSize==0){
					startTime = System.currentTimeMillis();

					pWriter.commit();	
					pWriter.close();
					pWriter.reconnect();

					long timeSpent = (System.currentTimeMillis()-startTime);
					logger.info("upsert " + CommitBatchSize + " rows for " +timeSpent+ " milliseconds ");
				}
			}

			pWriter.commit();
			pWriter.close();
			pWriter.reconnect();


			System.out.println("Shutting down Thread: " + m_threadNumber);
		}
	}

	public void run(int a_numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		// now launch all the threads
		//
		executor = Executors.newFixedThreadPool(a_numThreads);

		// now create an object to consume the messages
		//
		int threadNumber = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new ConsumerThread(stream, threadNumber));
			threadNumber++;
		}
	}

	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		return new ConsumerConfig(props);
	}


}
