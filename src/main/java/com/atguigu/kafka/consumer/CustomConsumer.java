package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;


public class CustomConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        //配置消费者参数 kafka集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-01:9092");
        //反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //没有提交offset 会从上次的offset开始消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");   //false:不自动提交offset
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "1205");//消费者组，只要group.id相同，就属于同一个消费者组

        //1.创建1个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费哪个主题
        consumer.subscribe(Arrays.asList("first"));

        //2.调用poll 拉取
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            //遍历获取数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic = " + record.topic() + " offset = " + record.offset() + " value = " + record.value());
            }

            /**
             * TODO 设置了提交,Consumer下次读取从提交的地方offset开始读取
             * 1.比如度一批数据0 1 2 3 4 5 设置提交
             * 2.下次Consumer再开启就从5之后读取了
             * 3.不设置提交,在读取数据 6 7 8 9
             * 4.再次开启Consumer 会从5之后读取 6 7 8 9
             *
             */
            //提交两种方式
            //异步就提交一次
            //consumer.commitAsync();
            //同步会失败重试
//            consumer.commitSync();  //一批消费完提交一次
        }
    }

}
