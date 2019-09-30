package com.atguigu.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
//自定义offset保存在外部,不使用kafka的 需要ConsumerRebalanceListener

public class CustomOffsetConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop-01:9092");
        props.put("group.id", "test");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put("enable.auto.commit", "false");//不自动提交offset
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            /**
             * TODO 自定义offset需要以下两个方法
             */
            //提交当前负责的分区的offset
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("回收的分区");
                for (TopicPartition partition : partitions) {
                    System.out.println("partition: "+partition);
                }
            }

            //定位新分配的分区的offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("重新的分区");
                for (TopicPartition partition : partitions) {
                    //获取topic下分区的offset
                    Long offset = getPartitionOffset(partition);
                    consumer.seek(partition,offset);
                }
            }
        });


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //以下放入事务中 保证Exactly Once
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                commitOffset(topicPartition,record.offset()+1);
            }
        }
    }
    //往外部保存offset的地方进行提交
    private static void commitOffset(TopicPartition topicPartition, long l) {

    }
    //定义offset存储外部哪里
    private static Long getPartitionOffset(TopicPartition partition) {
        return null;
    }

}
