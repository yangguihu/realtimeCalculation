package com.gome.bigData.bi.topologys.licai;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by MaLi on 2017/1/3.
 */
public class Topology_Licai {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new Spout_Licai());
        builder.setBolt("bolt1",new MessageParsingBolt(),5).shuffleGrouping("spout");
        builder.setBolt("bolt2",new IndexCalculateBolt(),1).shuffleGrouping("bolt1");

        Config config = new Config();
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
    }
}
