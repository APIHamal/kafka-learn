package com.lizhengpeng.kafka.learn.step2;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * kafka基础消费这演示
 * @author idealist
 */
public class BaseConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.168.168:9092");
        //配置消费这自定义的反序列组件
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDecoderSerializer.class);
        /**
         * 配置消费者的组ID(KAFKA使用组ID来实现负责均衡和[再均衡])
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "user_topic_group");
        KafkaConsumer<String,User> userKafkaConsumer = new KafkaConsumer<String, User>(properties);
        userKafkaConsumer.subscribe(Collections.singleton("channel_pub_sub"));
        logger.info("消费者开始监听KAFKA通道.....");
        while(true){
            ConsumerRecords<String,User> userConsumerRecords = userKafkaConsumer.poll(Duration.ofSeconds(5));
            for(ConsumerRecord<String,User> record : userConsumerRecords){
                User user = record.value();
                logger.info("接受到消息->年龄:"+user.getAge()+" 姓名->"+user.getName()+" 性别->"+user.getSex());
                userKafkaConsumer.commitSync();
            }
        }
    }

}
