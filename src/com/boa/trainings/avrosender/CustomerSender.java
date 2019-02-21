package com.boa.trainings.avrosender;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.boa.trainings.avroschema.CustomerSchema;




public class CustomerSender {
	public static void main(String[] args) {
		String schemaUrl = "http://localhost:8081";
		Properties props = new Properties();
		props.setProperty("schema.registry.url", schemaUrl);
		props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.setProperty("bootstrap.servers","localhost:9092");
		KafkaProducer<String,GenericRecord> producer = new KafkaProducer<>(props);
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(CustomerSchema.SCHEMA_DEFINITION);
		GenericRecord customer = new GenericData.Record(schema);
		customer.put("id",1002);
		customer.put("name","ram");
		ProducerRecord<String,GenericRecord> record =new ProducerRecord<>("customerTopic","customer1",customer);
		producer.send(record);
		System.out.println("message sent");
		producer.close();
	}
}
