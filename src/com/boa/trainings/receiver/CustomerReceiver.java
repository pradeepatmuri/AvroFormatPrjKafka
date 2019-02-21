package com.boa.trainings.receiver;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.boa.trainings.domain.Customer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomerReceiver {
	
	public static void main(String[] args) {
		String schemaUrl="http://localhost:8081";
		Properties props=new Properties();
		props.setProperty("schema.registry.url", schemaUrl);
		props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "demo-group");
		KafkaConsumer<String, GenericRecord> consumer=new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("customerTopic"));
		while(true) {
		ConsumerRecords<String, GenericRecord> records=consumer.poll(Duration.ofSeconds(3));
		for(ConsumerRecord<String, GenericRecord> record:records){
			System.out.println(record.value());
			ObjectMapper mapper=new ObjectMapper();
			try {
				Customer customer=mapper.readValue(record.value().toString().getBytes(), Customer.class);
				//System.out.println("type casted");
				System.out.println(customer.getId()+"\t"+customer.getName());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		}
		
	}

}
