package cn.edu360.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumerDemo {

    private static final String topic = "xiaoniu";
    private static final Integer threads = 2;

    public static void main(String[] args) {

        Properties props = new Properties();
        //指定ZK地址  老版本
        props.put("zookeeper.connect", "node-10:2181,node-11:2181,node-12:2181");
        //组
        props.put("group.id", "vvvvv");
        //smallest重最开始消费,largest代表重消费者启动后产生的数据才消费
        //相当于 --from-beginning 从头开始消费
        props.put("auto.offset.reset", "smallest");

        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector consumer =Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        //2个线程 阻塞
        for(final KafkaStream<byte[], byte[]> kafkaStream : streams){
            new Thread(new Runnable() {
                public void run() {
                    for(MessageAndMetadata<byte[], byte[]> mm : kafkaStream){
                        String msg = new String(mm.message());
                        System.out.println(msg);
                    }
                }
            }).start();
        }
    }
}
