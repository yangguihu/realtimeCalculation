package com.gome.bigData.bi.topologys.licai;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSON;
import com.gome.bigData.bi.db.mysql.GomeQueryRunner;
import com.gome.bigData.bi.db.redis.RedisConnector;
import com.gome.bigData.bi.tools.DateTools;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by MaLi on 2017/1/4.
 * 功能:
 * 1,解析将获取到的json数据
 * 2,过滤符合条件的消息,分发给下一级去处理
 * 3,缓存数据:
 * 3.1-当日注册用户数据存放在Redis 1#库 key为customer_id
 * 3.2-当日注册并鉴权数据存放在Redis 2#库 key为customer_id
 */
public class MessageParsingBolt implements IRichBolt {
    private Map conf = null;
    private TopologyContext context = null;
    private OutputCollector collector = null;
    private RedisConnector jedis = null;
    private GomeQueryRunner queryRunner = new GomeQueryRunner();

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = stormConf;
        this.context = context;
        this.collector = collector;
        // TODO: 2017/3/1 config
        jedis = new RedisConnector("10.143.90.106", 6379);
        //jedis = new RedisConnector("10.143.90.38", 6379);
    }

    public void execute(Tuple input) {
        /**
         //**在自带KafkaSpout里面获取数据
         //获取上一级Spout发送到的字节消息数据
         byte[] binary = input.getBinary(0);
         //还原成为字符串供后面环节处理
         String value = new String(binary);
         */
        //**在自定义的KafkaSpout里面获取数据
        String value = input.getStringByField("licai_messages");
        Map<String, Object> map = null;
        try {
            map = (Map<String, Object>) JSON.parse(value);
        } catch (Exception e) {
            collector.ack(input);
            return;
        }
        String schemaName = (String) map.get("SCHEMANAME");
        System.out.println("MessageParsingBolt Bolt接收到消息的数据库名称: " + schemaName);
        String eventType = (String) map.get("EVENTTYPE");
        System.out.println("MessageParsingBolt Bolt接收到消息的EventType: " + eventType);
        //01 index_01 注册用户数
        registerQuantity(map);
        //02 index_02 充值用户数
        rechargeQuantity(map);
        //03 index_03 新增注册且充值用户数
        rechargeQuantity2(map);
        //04 index_04 投资用户数
        investQuantity(map);
        //05 index_05 首投用户数

        //06 index_06 提现用户数
        withdrawQuantity(map);
        //07 index_07 提现金额
        withdrawAmount(map);
        //08 index_08 投资金额
        investAmount(map);
        //09 index_09 首投金额
        //实时计算没有办法计算这笔投资是否是首次投资

    }

    //08 index_08 投资金额
    private void investAmount(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String timerecorded = (String) map.get("TIMERECORDED");
        String status = (String) map.get("STATUS");
        String operation = (String) map.get("OPERATION");
        String type = (String) map.get("TYPE");

        if ("Biz".equalsIgnoreCase(schemaname) && "TB_FUND_RECORD".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && "OUT".equalsIgnoreCase(operation) && "INVEST".equalsIgnoreCase(type)&&"INITIALIZED".equalsIgnoreCase(status) && DateTools.isToday(timerecorded)) {
            //在中间表中保存主键和对应的金额
            String id = (String) map.get("ID");
            String amount = (String) map.get("AMOUNT");
            HashMap<String, Object> keyValue = new HashMap<>();
            keyValue.put(id, amount);
            Boolean flag = jedis.existsInHash("mid_licai_investAmount", id);
            if(!flag){
                jedis.save2hash("mid_licai_investAmount", keyValue);
                collector.emit(new Values("investAmount", id));//下一级收到id后,直接去中间表中读取金额,累加到已有的总额里面
            }
        }
    }

    //07 index_07 提现金额
    private void withdrawAmount(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String timerecorded = (String) map.get("TIMERECORDED");
        //String status = (String) map.get("STATUS");
        String operation = (String) map.get("OPERATION");
        String type = (String) map.get("TYPE");

        if ("Biz".equalsIgnoreCase(schemaname) && "TB_FUND_RECORD".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && "OUT".equalsIgnoreCase(operation) && "WITHDRAW".equalsIgnoreCase(type) && DateTools.isToday(timerecorded)) {
            //在中间表中保存主键和对应的金额
            String id = (String) map.get("ID");
            String amount = (String) map.get("AMOUNT");
            //首先判断中间表中是否有这个主键字段
            Boolean mid_licai_withdrawAmount = jedis.existsInHash("mid_licai_withdrawAmount", id);
            //如果不存在这个值,那么就保存这个主键id和其对应的值到中间表,并发送信号给下一级bolt,通知其进行计算
            if(!mid_licai_withdrawAmount){
                HashMap<String, Object> keyValue = new HashMap<>();
                keyValue.put(id, amount);
                jedis.save2hash("mid_licai_withdrawAmount", keyValue);
                collector.emit(new Values("withdrawAmount", id));//下一级收到id后,直接去中间表中读取金额,累加到已有的总额里面
            }
        }
    }

    //06 index_06 提现用户数
    private void withdrawQuantity(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String timerecorded = (String) map.get("TIMERECORDED");
        //String status = (String) map.get("STATUS");
        String operation = (String) map.get("OPERATION");
        String type = (String) map.get("TYPE");
        if ("Biz".equalsIgnoreCase(schemaname) && "TB_FUND_RECORD".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && "OUT".equalsIgnoreCase(operation) && "WITHDRAW".equalsIgnoreCase(type) && DateTools.isToday(timerecorded)) {
            String user_id = (String) map.get("USER_ID");
            //计算逻辑,保存主键号到redis临时表中: redis临时key名: mid_licai_rechargeQuantity
            jedis.save2set("mid_licai_withdrawQuantity", user_id);
            //通知下一级Bolt计算
            collector.emit(new Values("withdrawQuantity", user_id));
        }
    }

    //04 index_04 投资用户数
    private void investQuantity(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String timerecorded = (String) map.get("TIMERECORDED");
        String rootloanid = (String) map.get("ROOTLOANID");
        String status = (String) map.get("STATUS");
        String operation = (String) map.get("OPERATION");
        String type = (String) map.get("TYPE");
        if ("Biz".equalsIgnoreCase(schemaname) && "TB_FUND_RECORD".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && "INITIALIZED".equalsIgnoreCase(status) && "OUT".equalsIgnoreCase(operation) && "INVEST".equalsIgnoreCase(type) && DateTools.isToday(timerecorded)) {
            String user_id = (String) map.get("USER_ID");
            //计算逻辑,保存主键号到redis临时表中: redis临时key名: mid_licai_rechargeQuantity
            jedis.save2set("mid_licai_investQuantity", user_id);
            //通知下一级Bolt计算
            collector.emit(new Values("investQuantity", user_id));
        }
    }

    //03 index_03 新增注册且充值用户数
    private void rechargeQuantity2(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String timerecorded = (String) map.get("TIMERECORDED");
        if ("Biz".equalsIgnoreCase(schemaname) && "TB_FUND_RECORD".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && DateTools.isToday(timerecorded)) {
            String user_id = (String) map.get("USER_ID");
            Boolean mid_licai_registerQuantity = jedis.existsInSet("mid_licai_registerQuantity", user_id);
            if (mid_licai_registerQuantity) {
                //计算逻辑,保存主键号到redis临时表中: redis临时key名: mid_licai_rechargeQuantity2
                jedis.save2set("mid_licai_rechargeQuantity2", user_id);
                //通知下一级Bolt计算
                collector.emit(new Values("rechargeQuantity2", user_id));
            }
        }
    }

    //02 index_02 充值用户数
    private void rechargeQuantity(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String timerecorded = (String) map.get("TIMERECORDED");
        String status = (String) map.get("STATUS");
        String operation = (String) map.get("OPERATION");
        String type = (String) map.get("TYPE");

        if ("Biz".equalsIgnoreCase(schemaname) && "TB_FUND_RECORD".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && "SUCCESSFUL".equalsIgnoreCase(status) && "IN".equalsIgnoreCase(operation) && "DEPOSIT".equalsIgnoreCase(type) && DateTools.isToday(timerecorded)) {
            String user_id = (String) map.get("USER_ID");
            //计算逻辑,保存主键号到redis临时表中: redis临时key名: mid_licai_rechargeQuantity
            jedis.save2set("mid_licai_rechargeQuantity", user_id);
            //通知下一级Bolt计算
            collector.emit(new Values("rechargeQuantity", user_id));
        }
    }

    //01 注册用户数
    private void registerQuantity(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String registerdate = (String) map.get("REGISTERDATE");
        if ("Biz".equalsIgnoreCase(schemaname) && "TB_USER".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && DateTools.isToday(registerdate)) {
            String id = (String) map.get("ID");
            //计算逻辑,保存主键号到redis临时表中: redis临时key名: mid_licai_registerQuantity
            jedis.save2set("mid_licai_registerQuantity", id);
            //通知下一级Bolt计算
            collector.emit(new Values("registerQuantity", id));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("IndexName", "IndexValue"));
    }

    public void cleanup() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
