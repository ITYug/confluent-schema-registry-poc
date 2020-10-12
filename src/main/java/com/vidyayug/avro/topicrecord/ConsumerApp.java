package com.vidyayug.avro.topicrecord;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static io.confluent.kafka.serializers.KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG;
import static java.text.MessageFormat.format;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.vidyayug.avro.Credential;
import com.vidyayug.avro.Creditcard;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;

public class ConsumerApp {

	static final String BOOTSTRAP_SERVERS = "localhost:9092";
	static final String CONSUMER_GROUP_ID = "local";
	static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
		props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		props.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		props.put(SPECIFIC_AVRO_READER_CONFIG, true);
		props.put(VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());

		try (KafkaConsumer<String, SpecificRecord> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Collections.singleton("topic2"));

			while (true) {
				final ConsumerRecords<String, SpecificRecord> records = consumer.poll(Duration.ofSeconds(1));
				records.forEach(record -> {
					final SpecificRecord value = record.value();
					System.out.println(format("got record: {0}", value));

					if (value instanceof Credential) {
						System.out.println(format("credential {0} received", ((Credential) value).getEmail()));
					} else if (value instanceof Creditcard) {
						System.out.println(format("Creditcard {0} received", ((Creditcard) value).getNumber()));
					} else {
						System.out.println(format("something else happened: {0}", record));
					}
				});
			}
		}
	}

}
