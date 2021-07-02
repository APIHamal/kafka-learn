package com.lizhengpeng.kafka.learn.step2;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 配置自定义的分区选择器
 * @author idealist
 */
public class CustomPartitioner implements Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);

    @Override
    public int partition(String topic, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        /**
         * Cluster对象的partitionsForTopic方法可以获取当前主题拥有的分区的信息
         * 获取分区消息后可以根据不同的算法对分区进行负载均衡操作
         */
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        logger.info("当前可用的分区数量->"+partitionInfoList.size());
        /**
         * 该演示使用的方法总是返回最后一个分区
         * 因此Producer发送的消息都会发往该topic创建的最后一个分区中
         * broker中对topic分区的实现实际上就是一个日志文件
         */
        return partitionInfoList.size()-1;
    }

    @Override
    public void close() {
        /**
         * 方法用于清理相关的资源
         * 同序列化/反序列化组件相同
         */
    }

    @Override
    public void configure(Map<String, ?> map) {
        /**
         * 该方法用于配置相关信息
         * 同序列化/反序列化组件相同
         * 当生产者实例化时会创建相应的分区管理器并且
         * 会调用该方法将Map传入(Map实际上是Properties对象转换而来
         * 等于是Producer对象调用了Partitioner对象的configure方法)
         */
    }
}
