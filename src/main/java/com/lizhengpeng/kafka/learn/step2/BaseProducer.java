package com.lizhengpeng.kafka.learn.step2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * kafka基础生产者
 * @author idealist
 */
public class BaseProducer {

    private static final Logger logger = LoggerFactory.getLogger(BaseProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.168.168:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,UserSerializer.class);
        properties.put(ProducerConfig.RETRIES_CONFIG,Integer.valueOf(3));
        properties.put(ProducerConfig.ACKS_CONFIG, String.valueOf(1));
        /**
         * 使用自定义的分区选择器
         * 该分区选择器总是会选择主题可用分区中的最后一个分区
         * 例如有5个分区则总是返回下标索引4
         */
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomPartitioner.class);
        KafkaProducer<String,User> kafkaProducer = new KafkaProducer<String, User>(properties);
        logger.info("发送数据消息....");
        for(int index = 0;index < 100;index++){
            User user = new User(String.valueOf("name->"+index),index,"max");
            ProducerRecord<String, User> record = new ProducerRecord<String, User>("user_topic",user);
            Future<RecordMetadata> future = kafkaProducer.send(record);
            future.get();
        }
        kafkaProducer.close();
        logger.info("数据消息发送完成");
    }

}
