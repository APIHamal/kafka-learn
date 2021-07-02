package com.lizhengpeng.kafka.learn.step2;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 自定义JSON序列化工具
 * @author idealist
 */
public class UserSerializer implements Serializer<User> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        /**
         * 用于加载配置数据(Properties中配置信息会转化成Map并且当)
         * Producer实例化时会调用该方法传入Properties转化的Map
         */
    }

    /**
     * 序列化方法将相关的对象序列化成字节数据
     * @param s
     * @param user
     * @return
     */
    @Override
    public byte[] serialize(String s, User user) {
        if(user == null){
            return new byte[0];
        }
        String userJson = JSON.toJSONString(user);
        return userJson.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        /**
         * 用于清理资源数据通常情况下不需要实现该方法
         */
    }
}
