package com.kafka;

import java.util.concurrent.ExecutionException;

import com.kafka.utilities.AdminUtilities;
import com.kafka.constants.IKafkaConstants;
import com.kafka.consumer.ConsumerCreator;
import com.kafka.producer.ProducerCreator;
import com.kafka.utilities.DebugUtilities;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

// Admin kafka imports



public class App {

	public static AdminUtilities admin = new AdminUtilities();
	public static DebugUtilities debug = new DebugUtilities();

	public static void main(String[] args) {

		int rc;

		// BASIC PROTOTYPE

		// 1) Produce one message to Queue0

		produceOneShot("QUEUE0","IT IS A TEST");

		// 2) Consume Message from QUEUE0

		consumeOneShot("QUEUE0");

		// 3) Run batch

		rc = 0;

		// 4) Produce message to QUEUE1 or QUEUE2
		rc = 404;

		if (rc == 500)
		{

		} else if (rc == 0)
		{
			produceOneShot("QUEUE1","IT IS A TEST");
		} else
		{
			produceOneShot("QUEUE2","IT IS A TEST");
		}

		admin.listTopics();

		//runProducer();
		//runConsumer();
	}

	static void consumeOneShot(String topic) {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer(topic);

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1);

			if (consumerRecords.count() == 1) {
				debug.printDebug("Record found in the topic " + topic + "\nContent:");
				printRecordsContent(consumerRecords);
				consumer.commitAsync();
				break;
			} else if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
				{
					debug.printDebug("No record found in the topic " + topic);
					break;
				}

				else
					continue;
			}
			consumer.commitAsync();
		}

		consumer.close();
	}

	static void consume(String topic) {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer(topic);

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1);
			debug.printDebug("Consuming the topic " + topic);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			printRecordsContent(consumerRecords);
			consumer.commitAsync();
		}
		consumer.close();
	}

	private static void printRecordsContent(ConsumerRecords<Long, String> consumerRecords) {
		consumerRecords.forEach(new java.util.function.Consumer<ConsumerRecord<Long, String>>() {
			@Override
			public void accept(ConsumerRecord<Long, String> record) {
				debug.printDebug("Record Key " + record.key());
				debug.printDebug("Record value " + record.value());
				debug.printDebug("Record partition " + record.partition());
				debug.printDebug("Record offset " + record.offset());
			}
		});
	}

	static void runProducer(String topic, String message) {
		Producer<Long, String> producer = ProducerCreator.createProducer();

		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic,
					message);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Error in sending record");
				System.out.println(e);
			}
		}
	}

	static void produceOneShot(String topic, String message) {
		Producer<Long, String> producer = ProducerCreator.createProducer();

		final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(topic,
				message);
		try {
			RecordMetadata metadata = producer.send(record).get();
			debug.printDebug("Record sent to topic " + metadata.topic() + " - partition " + metadata.partition()
					+ " with offset " + metadata.offset());
		} catch (ExecutionException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		} catch (InterruptedException e) {
			System.out.println("Error in sending record");
			System.out.println(e);
		}

		}
}
