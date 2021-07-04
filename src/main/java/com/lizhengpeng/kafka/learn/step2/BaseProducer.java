package com.lizhengpeng.kafka.learn.step2;

import org.apache.kafka.clients.producer.*;
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

    /**
     * 发送消息到指定的主题
     * @param topic
     */
    public void sendMessage(String topic) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.168.168:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,UserSerializer.class);
        properties.put(ProducerConfig.RETRIES_CONFIG,Integer.valueOf(3));
        /**
         * 配置生产者消息确认的机制
         */
        properties.put(ProducerConfig.ACKS_CONFIG, String.valueOf(1));
        /**
         * 配置生产者进行重试操作的次数以及两次重试操作的时间间隔
         * 时间间隔设置为500毫秒
         */
        properties.put(ProducerConfig.RETRIES_CONFIG,Integer.valueOf(3));
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
        /**
         * 发送顺序消息时必须设置该参数否则发生重试操作时会发生消息的乱序
         * 该属性表示每次指发送一个请求
         */
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,1);
        /**
         * 配置生产者请求等待broker响应的超时时间
         * 通常情况下该值应该相对设置的大一些防止由于误判
         * 造成客户端发生超时重试从而导致[重复消息的发生]
         * 设置超时的时间为5秒钟
         */
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 50000);
        /**
         * 使用自定义的分区选择器
         * 该分区选择器总是会选择主题可用分区中的最后一个分区
         * 例如有5个分区则总是返回下标索引4
         */
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,CustomPartitioner.class);
        KafkaProducer<String,User> kafkaProducer = new KafkaProducer<String, User>(properties);
        logger.info("发送数据消息....");
        for(int index = 0;index < 100000;index++){
            User user = new User(String.valueOf("name->"+index),index,"max");
            ProducerRecord<String, User> record = new ProducerRecord<String, User>(topic,user);
            Future<RecordMetadata> future = kafkaProducer.send(record);
            future.get();
        }
        kafkaProducer.close();
        logger.info("数据消息发送完成");
    }

    /**
     * 测试运行主程序
     * @param args
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //发送指定数量的消息到队列中
        new Thread(() -> {
            try {
                new BaseProducer().sendMessage("channel_pub_sub");
            } catch (Exception e) {
                logger.info("消息发送出现异常", e);
            }
        }).start();
        new Thread(() -> {
            try {
                new BaseProducer().sendMessage("mesg_topic");
            } catch (Exception e) {
                logger.info("消息发送出现异常", e);
            }
        }).start();
    }

}
