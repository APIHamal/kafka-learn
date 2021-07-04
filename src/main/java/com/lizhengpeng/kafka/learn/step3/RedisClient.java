package com.lizhengpeng.kafka.learn.step3;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * redis客户端的配置
 * @author idealist
 */
public class RedisClient {

    /**
     * jedis相关配置对象
     */
    private JedisSentinelPool jedisSentinelPool;
    private ThreadLocal<Jedis> jedisThreadLocal = new ThreadLocal<>();

    /**
     * 默认的构造函数
     */
    public RedisClient(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(6);
        Set<String> hostSet = new HashSet<>();
        hostSet.addAll(Arrays.asList("192.168.168.168:26379","192.168.168.169:26379","192.168.168.170:26379"));
        jedisSentinelPool = new JedisSentinelPool("890cluster", hostSet, jedisPoolConfig);
    }

    /**
     * 获取Jedis对象
     * @return
     */
    public Jedis getJedisClient(){
        Jedis client = jedisThreadLocal.get();
        if(client == null){
            client = jedisSentinelPool.getResource();
            jedisThreadLocal.set(client);
        }
        return client;
    }

    /**
     * 释放当前线程持有的Jedis连接
     */
    public void releaseJedisClient(){
        Jedis client = jedisThreadLocal.get();
        if(client != null){
            jedisThreadLocal.remove();
            client.close();
        }
    }

}
