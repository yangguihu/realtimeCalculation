package com.gome.bigData.bi.topologys.fen;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.gome.bigData.bi.kafka.consumer.KafkaConsumer;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by MaLi on 2017/2/26.
 */
public class Spout_Fen implements IRichSpout {
    private SpoutOutputCollector collector;
    private Queue<String> queue = new ConcurrentLinkedQueue<String>() ;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        // TODO: 2017/3/1 config
        //String zkHost = "10.143.90.102:2181,10.143.90.103:2181,10.143.90.104:2181";
        String zkHost = "10.143.90.38:2181,10.143.90.39:2181,10.143.90.49:2181";
        //创建KafkaConsumer;
        KafkaConsumer consumer = new KafkaConsumer(zkHost, "realtime_fen_group", "realtime_fen");
        consumer.start();
        queue = consumer.getQueue();
    }

    @Override
    public void nextTuple() {
        if(queue.size()>0){
            String message = queue.poll();
            collector.emit(new Values(message));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("fen_message"));
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }


    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
