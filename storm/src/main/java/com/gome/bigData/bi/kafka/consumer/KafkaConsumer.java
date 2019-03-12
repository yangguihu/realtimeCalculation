package com.gome.bigData.bi.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by MaLi on 2017/2/26.
 */
public class KafkaConsumer extends Thread {
    //创建consumer的步骤1,创建ConsumerConfig-->得到ConsumerConnector-->得到MessageStream
    private final ConsumerConnector consumer;
    private String topic;
    private Queue<String> queue = new ConcurrentLinkedQueue<String>() ;

    public KafkaConsumer(String zkHosts,String groupId,String topicName) {
        Properties props = new Properties();
        //定义连接zookeeper信息
        props.put("zookeeper.connect", zkHosts);
        //定义Consumer所有的groupID，关于groupID，后面会继续介绍
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        this.topic = topicName;
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    //创建一个消费方法
    public void run(){
        Map<String, Integer> topicMap = new HashMap<String,Integer>();
        topicMap.put(topic,new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicMap);
        KafkaStream<byte[], byte[]> kafkaStream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        while(it.hasNext()){
            String message = new String(it.next().message());
            queue.add(message);
            System.err.println("Consumer is running... : "+message);
        }
    }

    public Queue<String> getQueue() {
        return queue;
    }


    public static void main(String[] args){
        KafkaConsumer consumer = new KafkaConsumer("10.143.90.38:2181,10.143.90.39:2181,10.143.90.49:2181","group0410","test");
        consumer.start();
        String poll = consumer.queue.poll();
        System.out.println(poll);
    }
}
