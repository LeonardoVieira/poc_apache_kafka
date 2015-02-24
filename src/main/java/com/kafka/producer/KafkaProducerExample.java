package com.kafka.producer;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import com.kafka.Constants;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerExample {

	public static void main(final String[] args) throws IOException {
		long events = Long.parseLong("5");
        Random rnd = new Random();
        Properties props = new Properties();

        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.kafka.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip;
               KeyedMessage<String, String> data = new KeyedMessage<String, String>(Constants.TOPIC_NAME, ip, msg);
               producer.send(data);
        }

        producer.close();
	}
}