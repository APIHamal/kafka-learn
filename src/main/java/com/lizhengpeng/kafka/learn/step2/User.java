package com.lizhengpeng.kafka.learn.step2;

import lombok.Getter;
import lombok.Setter;

/**
 * 测试使用的Model对象
 * @author idealist
 */
@Setter
@Getter
public class User {

    private String name;
    private Integer age;
    private String sex;

    public User(String name, Integer age, String sex){
        this.name = name;
        this.age = age;
        this.sex = sex;
    }

}
