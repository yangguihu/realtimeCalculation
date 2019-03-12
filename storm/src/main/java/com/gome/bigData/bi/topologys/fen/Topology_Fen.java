package com.gome.bigData.bi.topologys.fen;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by MaLi on 2017/1/3.
 */
public class Topology_Fen {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new Spout_Fen(),1);
        builder.setBolt("bolt", new MessageParsingBolt(), 5).shuffleGrouping("spout");
        builder.setBolt("bolt2", new IndexCalculateBolt()).shuffleGrouping("bolt");
        //LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        //cluster.submitTopology("Topotest1122", config, builder.createTopology());
        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("LocalTopoTest_fen", config, builder.createTopology());
        }

//         //ZkHosts hosts = new ZkHosts("192.168.204.128:2181,192.168.204.129:2181,192.168.204.130:2181");
//         ZkHosts hosts = new ZkHosts("10.143.90.38:2181,10.143.90.39:2181,10.143.90.49:2181");
//         SpoutConfig spoutConfig = new SpoutConfig(hosts, "test", "/root_kafkaSpout", "kafkaspout");
//         spoutConfig.forceFromStart = true;   //在kafka的最早offset开始消费数据
//         //spoutConfig.startOffsetTime =kafka.api.OffsetRequest.LatestTime();
//         spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
//
//         HashMap<String, String> map = new HashMap<String, String>();
//         map.put("metadata.broker.list", "10.143.90.38:9092,10.143.90.39:9092,10.143.90.49:9092");
//         map.put("serializer.class", "kafka.serializer.StringEncoder");
//
//         Config config = new Config();
//         config.put("kafka.broker.properties", map);
//         config.put("topic", "test");
//
//         SchemeAsMultiScheme scheme = new SchemeAsMultiScheme(new MessageScheme());
//        //集群模式运行or本地模式运行
//        if (args.length > 0) {
//            try {
//                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
//            } catch (AlreadyAliveException e) {
//                e.printStackTrace();
//            } catch (InvalidTopologyException e) {
//                e.printStackTrace();
//            }
//        } else {
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("Topotest1121", config, builder.createTopology());
//        }
    }
}
