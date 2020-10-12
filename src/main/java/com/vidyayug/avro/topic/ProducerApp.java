package com.vidyayug.avro.topic;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.vidyayug.avro.Credential;
import com.vidyayug.avro.Creditcard;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

public class ProducerApp {

	static final String BOOTSTRAP_SERVERS = "localhost:9092";
	static final String CONSUMER_GROUP_ID = "locall";
	static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

	public static void main(String[] args) throws InterruptedException {

		// configure properties
		Properties props = new Properties();
		props.setProperty(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		props.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

		// set topc name strategegy
		props.put(VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());

		try (KafkaProducer<String, SpecificRecord> producer = new KafkaProducer<>(props)) {
			String topic1 = "topic1";
			String email = "c@gmail.com";
			final Credential credential = buildCredential(email, "123456");
			ProducerRecord<String, SpecificRecord> producerRecord1 = new ProducerRecord<String, SpecificRecord>(
					topic1, email, credential);
			producer.send(producerRecord1);
			System.out.println("Published credentials");


		}
	}

	private static Credential buildCredential(final String email, final String password) {
		return Credential.newBuilder().setEmail(email).setPassword(password).build();
	}

	private static Creditcard buildCreditcard(final String number, final String cvv) {
		return Creditcard.newBuilder().setNumber(number).setCvv(cvv).build();
	}

}
