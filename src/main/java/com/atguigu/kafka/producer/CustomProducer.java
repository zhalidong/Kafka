package com.atguigu.kafka.producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 异步发送  发送消息到RecordAccumulator 不返回ack也继续发
 */

public class CustomProducer {

    public static void main(String[] args) throws InterruptedException {
        //配置生产者参数
        Properties props = new Properties();
        //kafka集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop-01:9092");
        //key value 序列化全类名
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        //把两个拦截器放入一个list中
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu.kafka.interceptor.CounterInterceptor");
        interceptors.add("com.atguigu.kafka.interceptor.TimeInterceptor");
        //指定拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);


        //1.创建1个生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //2.调用send方法
        for (int i = 0; i < 100; i++) {
            Thread.sleep(10);
            producer.send(new ProducerRecord<String, String>("number", i + "", "message-" + i));
        }

        //3.关闭生产者
        producer.close();
    }
}
