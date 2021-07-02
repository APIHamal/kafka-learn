package com.lizhengpeng.kafka.learn.base;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.168.168:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int index = 0;index < 100;index++){
            ProducerRecord<String,String> tmpRecord = new ProducerRecord<String,String>("first_topic",String.valueOf(index));
            Future<RecordMetadata> future = producer.send(tmpRecord);
            if(future.get() != null){
                RecordMetadata metadata = future.get();
                System.out.println("消息发送完成"+metadata.topic()+"  "+metadata.partition()+"  "+metadata.offset());
            }
        }
        producer.close();
    }
}
