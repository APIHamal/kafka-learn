package com.lizhengpeng.kafka.learn.step3;

import com.lizhengpeng.kafka.learn.step2.User;
import com.lizhengpeng.kafka.learn.step2.UserDecoderSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 自定义分区管理对象
 * @author idealist
 */
public class CustomerKafkaConsumer implements ConsumerRebalanceListener {

    /**
     * 日志消息输出
     */
    private static final Logger logger = LoggerFactory.getLogger(CustomerKafkaConsumer.class);

    /**
     * 控制当前消费程序的运行
     */
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * 指定Kafka的消费者对象
     */
    private KafkaConsumer<String, User> kafkaConsumer;

    /**
     * 分区消息管理
     */
    private TopicPartitionHolder topicPartitionHolder;

    /**
     * 默认的构造函数
     * @param kafkaCluster
     */
    public CustomerKafkaConsumer(String kafkaCluster, TopicPartitionHolder topicPartitionHolder){
        this.topicPartitionHolder = topicPartitionHolder;
        initConsumer(kafkaCluster);
    }

    /**
     * 初始化当前的消费者对象
     * @param kafkaCluster
     */
    private void initConsumer(String kafkaCluster){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaCluster);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "customer_group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDecoderSerializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        kafkaConsumer = new KafkaConsumer<String, User>(properties);
    }

    /**
     * 自定义消费指定的分区
     * @param topic
     */
    public void subscribeTopic(String topic) throws Exception {
        /**
         * 分区再均衡发生时重新从上次提交的偏移量开始获取数据
         * 偏移量保存在Redis服务中(注意redis的复制特性存在延迟
         * 情况因此当master节点宕机后sentinel重新选举出新master
         * 节点时可能会丢失来不及复制的数据)
         */
        kafkaConsumer.subscribe(Arrays.asList(topic),this);
        /**
         * 注意:调用该方法是为了使broker进行消费者的分区分配(不能直接查询主题的分区信息
         * 因为当消费者组存在多个消费者时分区的分配存在不确定性
         * 因此直接消费分区指定偏移量的数据时需要明确当前broker分配给自己的分区方案)
         */
        kafkaConsumer.poll(Duration.ofSeconds(5));
        Set<TopicPartition> partitionSet = kafkaConsumer.assignment();
        /**
         * 遍历分区方法查询redis中的偏移量数据
         * 若偏移量不存在则直接从0开始进行消费
         */
        for(TopicPartition topicPartition : partitionSet){
            /**
             * 从redis读取指定的偏移量
             * 注意:该方法是初始化消费者对象是第一次被调用
             */
            kafkaConsumer.seek(topicPartition,topicPartitionHolder.getOffset(topicPartition));
        }
        /**
         * 下面为消费消息的逻辑部分执行严格提交
         * 每消费一次消息进行一次提交(注意提交的消费的offset为下次待消费的偏移量
         * 即消息偏移量+1)
         */
        while(!shutdown.get()){
            ConsumerRecords<String,User> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String,User> record : consumerRecords){
                /**
                 * 业务逻辑处理部分:当前假设业务消费逻辑仅仅是打印数据信息即可进行数据提交
                 */
                User user = record.value();
                logger.info("消息体->"+user.getAge()+"->"+user.getName()+"->"+user.getSex());
                /**
                 * 业务逻辑处理完成后进行提交数据
                 * 注意提交的偏移量为当前消息偏移量的下一个位置
                 * 这里不会将偏移量提交到kafka中相当于使用redis接管了
                 * 整个分区的便宜量的提交
                 */
                topicPartitionHolder.submitOffset(new TopicPartition(record.topic(),record.partition()),record.offset()+1);
            }
        }
    }

    /**
     * 分区即将发生再均衡操作(分区发生再均衡时整个分区处于不可用的状态
     * 因此生产环境中应该精良避免发生分区再均衡的操作当数据量较大时
     * 会造成延迟)
     * 注意:该方法是将偏移量提交到kafka中(通常应该保持redis和kafka中的偏移量一致)
     * @param partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        try {
            for(TopicPartition topicPartition : partitions){
                /**
                 * 注意:根据消费原则offset始终指向待消费的消息偏移量
                 * 因此应该提交当前消费消息所在偏移量下一个位置
                 * 否则会发生消息的重复消费
                 */
                long offset = topicPartitionHolder.getOffset(topicPartition);
                kafkaConsumer.commitSync(Collections.singletonMap(new TopicPartition(topicPartition.topic(),topicPartition.partition()),
                        new OffsetAndMetadata(offset))
                );
            }
        }catch (Exception e){
            logger.error("分区再均衡提交偏移量失败",e);
        }
    }

    /**
     * 分区发生再均衡重新分配了分区数据时会调用该方法
     * @param partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        try {
            for(TopicPartition topicPartition : partitions){
                /**
                 * 注意:根据消费原则offset始终指向待消费的消息偏移量
                 * 因此应该提交当前消费消息所在偏移量下一个位置
                 * 否则会发生消息的重复消费
                 */
                long offset = topicPartitionHolder.getOffset(topicPartition);
                kafkaConsumer.seek(topicPartition, offset);
            }
        }catch (Exception e){
            logger.error("分区再均衡拉去指定消息失败",e);
        }
    }

    public static void main(String[] args) throws Exception {
        String cluster = "192.168.168.168:9092,192.168.168.169:9092,192.168.168.170:9092";
        RedisClient redisClient = new RedisClient();
        TopicPartitionHolder topicPartitionHolder = new TopicPartitionHolder(redisClient);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    CustomerKafkaConsumer consumer = new CustomerKafkaConsumer(cluster,topicPartitionHolder);
                    consumer.subscribeTopic("channel_pub_sub");
                }catch (Exception e){
                    logger.info("异步启用消费失败",e);
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    CustomerKafkaConsumer consumer = new CustomerKafkaConsumer(cluster,topicPartitionHolder);
                    consumer.subscribeTopic("channel_pub_sub");
                }catch (Exception e){
                    logger.info("异步启用消费失败",e);
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    CustomerKafkaConsumer consumer = new CustomerKafkaConsumer(cluster,topicPartitionHolder);
                    consumer.subscribeTopic("channel_pub_sub");
                }catch (Exception e){
                    logger.info("异步启用消费失败",e);
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    CustomerKafkaConsumer consumer = new CustomerKafkaConsumer(cluster,topicPartitionHolder);
                    consumer.subscribeTopic("mesg_topic");
                }catch (Exception e){
                    logger.info("异步启用消费失败",e);
                }
            }
        }).start();
    }

}
