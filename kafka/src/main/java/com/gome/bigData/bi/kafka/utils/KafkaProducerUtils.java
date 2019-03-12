package com.gome.bigData.bi.kafka.utils;

import common.Configs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by MaLi on 2017/1/3.
 */
public class KafkaProducerUtils {
    private static Properties props = new Properties();
    static{
        String brokerServers = Configs.get("bootstrap.servers");
        props.put("bootstrap.servers", brokerServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
    }
    public static void send(String topicName, Object key, Object value) {
        KafkaProducer producer = new KafkaProducer(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord(topicName, key, value);
        producer.send(producerRecord);
        producer.close();
    }
}
