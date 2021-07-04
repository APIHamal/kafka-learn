package com.lizhengpeng.kafka.learn.step3;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import redis.clients.jedis.Jedis;

/**
 * 主题分区的偏移量的管理
 * @author idealist
 */
public class TopicPartitionHolder {

    /**
     * Redis客户端对象
     */
    private RedisClient redisClient;

    /**
     * 默认的构造函数
     * @param redisClient
     */
    public TopicPartitionHolder(RedisClient redisClient){
        this.redisClient = redisClient;
    }

    /**
     * 计算分区信息对应redis中的键名称
     * @param topicPartition
     * @return
     */
    private String getTopicPartitionKey(TopicPartition topicPartition){
        long threadId = Thread.currentThread().getId();
        return topicPartition.topic()+"_"+topicPartition.partition()+"_thread_"+threadId;
    }

    /**
     * 获取当前指定分区的偏移量
     * @param topicPartition
     * @return
     */
    public Long getOffset(TopicPartition topicPartition) throws Exception{
        try{
            if(topicPartition == null){
                throw new IllegalArgumentException("分区信息为空");
            }
            Jedis jedis = redisClient.getJedisClient();
            String offsetStr = jedis.get(getTopicPartitionKey(topicPartition));
            if(offsetStr == null){
                return 0L;
            }
            return Long.valueOf(offsetStr);
        }catch (Exception e){
            throw e;
        }finally {
            redisClient.releaseJedisClient();
        }
    }

    /**
     * 设置指定分区的偏移量
     * @param topicPartition
     * @param offset
     */
    public void submitOffset(TopicPartition topicPartition,long offset) throws Exception{
        try{
            if(topicPartition == null){
                throw new IllegalArgumentException("分区信息为空");
            }
            if(offset < 0){
                throw new IllegalArgumentException("偏移量参数错误");
            }
            Jedis jedis = redisClient.getJedisClient();
            jedis.set(getTopicPartitionKey(topicPartition),String.valueOf(offset));
        }catch (Exception e){
            throw e;
        }finally {
            redisClient.releaseJedisClient();
        }
    }

}
