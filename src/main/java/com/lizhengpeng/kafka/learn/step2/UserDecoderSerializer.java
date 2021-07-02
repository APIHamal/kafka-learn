package com.lizhengpeng.kafka.learn.step2;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * kafka中自定义的反序列化组件
 * @author idealist
 */
public class UserDecoderSerializer implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        //同序列化组件中相同的作用
    }

    /**
     * 反序列化将字节数据转换成相应的对象
     * @param s
     * @param bytes
     * @return
     */
    @Override
    public User deserialize(String s, byte[] bytes) {
        if(bytes == null || bytes.length <= 0){
            User defaultUser = new User("-1",-1,"-1");
            return defaultUser;
        }
        String userJson = new String(bytes, StandardCharsets.UTF_8);
        return JSON.parseObject(userJson,User.class);
    }

    @Override
    public void close() {
        //同序列化中相同的作用
    }
}
