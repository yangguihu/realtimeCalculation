package com.gome.bigData.bi.topologys.licai;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.gome.bigData.bi.db.mysql.DBUtils;
import com.gome.bigData.bi.db.redis.RedisConnector;
import com.gome.bigData.bi.tools.DateTools;
import redis.clients.jedis.Jedis;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by MaLi on 2017/1/5.
 */
public class IndexCalculateBolt implements IRichBolt {
    private Map conf = null;
    private TopologyContext context = null;
    private OutputCollector collector = null;
    // TODO: 2017/3/1 config
    private RedisConnector connector = new RedisConnector("10.143.90.106", 6379);
    //private RedisConnector connector = new RedisConnector("10.143.90.39", 6379);

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = stormConf;
        this.context = context;
        this.collector = collector;
    }

    public void execute(Tuple input) {
        //***** 获取Jedis连接,准备保存指标名称和指标值到Hash数据结构 key: realtime_fen   value: 指标名称:指标值
        Jedis jedis = connector.getJedis();
        //01 判断是否需要清空redis数据,提取时间字段,
        String current_date_in_redis = connector.getFromHash("realtime_licai", "currentDate");//全格式时间
        // TODO: 2017/2/27 数据库存储需要修改
        if (!DateTools.isToday(current_date_in_redis)) {//如果和今天不等,那么清空数据库
            /*//01保存数据到mysql数据库
            DataSource canal_test = DBUtils.getDataSource("canal_test");//保存数据到关系数据库里面
            Connection connection = null;
            PreparedStatement statement = null;
            try {
                //1,提取redis中realtime_index_fen中的指标数据
                //2,保存数据到mysql
                connection = canal_test.getConnection();
                String sql_saveIndex = "INSERT INTO realtime_index_licai (currentDates, registerQuantity,rechargeQuantity,rechargeQuantity2,investQuantity,withdrawQuantity,withdrawAmount,investAmount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                statement = connection.prepareStatement(sql_saveIndex);
                statement.setString(1, jedis.hget("realtime_licai", "currentDate"));
                statement.setString(2, jedis.hget("realtime_licai", "registerQuantity"));
                statement.setString(3, jedis.hget("realtime_licai", "rechargeQuantity"));
                statement.setString(4, jedis.hget("realtime_licai", "rechargeQuantity2"));
                statement.setString(5, jedis.hget("realtime_licai", "investQuantity"));
                statement.setString(6, jedis.hget("realtime_licai", "withdrawQuantity"));
                statement.setString(7, jedis.hget("realtime_licai", "withdrawAmount"));
                statement.setString(8, jedis.hget("realtime_licai", "investAmount"));
                statement.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                DBUtils.release(connection, statement, null);
            }*/
            //02清空redis 0#数据库中的美易分指标库,多个临时中间表
            ArrayList<String> keys = new ArrayList<>();
            keys.add("realtime_licai");
            keys.add("mid_licai_registerQuantity");//
            keys.add("mid_licai_rechargeQuantity");
            keys.add("mid_licai_rechargeQuantity2");
            keys.add("mid_licai_investQuantity");
            keys.add("mid_licai_withdrawQuantity");
            keys.add("mid_licai_withdrawAmount");
            keys.add("mid_licai_investAmount");
            connector.deleteKeys(keys);
        }

        //01 获取上一级MessageParsingBolt发送过来的指标
        String indexName = input.getStringByField("IndexName");//美易分产品线指标名称
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //准备待插入数据
        HashMap<String, String> map = new HashMap<>();
        map.put("currentDate", format.format(new Date()));
        //目的创建key: realtime_index_fen
        if (!jedis.exists("realtime_fen"))
            jedis.hset("realtime_fen", "currentDate", format.format(new Date()));
        //根据不同的指标分别落库
        switch (indexName) {
            //01 index_01 注册用户数
            case "registerQuantity":
                //计算逻辑: 中间表中数据算总数
                //中间表: mid_licai_registerQuantity
                Long mid_licai_registerQuantity = jedis.scard("mid_licai_registerQuantity");
                //保存结果到hash: realtime_licai里面
                jedis.hset("realtime_licai", "registerQuantity", mid_licai_registerQuantity + "");
                jedis.hset("realtime_licai", "currentDate",format.format(new Date()));

                break;
            //02 index_02 充值用户数
            case "rechargeQuantity":
                //计算逻辑: 中间表中数据算总数
                //中间表: mid_licai_rechargeQuantity
                Long mid_licai_rechargeQuantity = jedis.scard("mid_licai_rechargeQuantity");
                //保存结果到hash: realtime_licai里面
                jedis.hset("realtime_licai", "rechargeQuantity", mid_licai_rechargeQuantity + "");
                jedis.hset("realtime_licai", "currentDate",format.format(new Date()));
                break;
            //03 index_03 新增注册且充值用户数
            case "rechargeQuantity2":
                //计算逻辑: 中间表中数据算总数
                //中间表: mid_licai_rechargeQuantity2
                Long mid_licai_rechargeQuantity2 = jedis.scard("mid_licai_rechargeQuantity2");
                //保存结果到hash: realtime_licai里面
                jedis.hset("realtime_licai", "rechargeQuantity2", mid_licai_rechargeQuantity2 + "");
                jedis.hset("realtime_licai", "currentDate",format.format(new Date()));
                break;
            //04 index_04 投资用户数
            case "investQuantity":
                //计算逻辑: 中间表中数据算总数
                //中间表: mid_licai_investQuantity
                Long mid_licai_investQuantity = jedis.scard("mid_licai_investQuantity");
                //保存结果到hash: realtime_licai里面
                jedis.hset("realtime_licai", "investQuantity", mid_licai_investQuantity + "");
                jedis.hset("realtime_licai", "currentDate",format.format(new Date()));
                break;
            //06 index_06 提现用户数
            case "withdrawQuantity":
                //计算逻辑: 中间表中数据算总数
                //中间表: mid_licai_withdrawQuantity
                Long mid_licai_withdrawQuantity = jedis.scard("mid_licai_withdrawQuantity");
                //保存结果到hash: realtime_licai里面
                jedis.hset("realtime_licai", "withdrawQuantity", mid_licai_withdrawQuantity + "");
                jedis.hset("realtime_licai", "currentDate",format.format(new Date()));
                break;
            //07 index_07 提现金额
            case "withdrawAmount":
                //计算逻辑:  获取到主键
                String id = input.getStringByField("IndexValue");
                //通过这个id去中间表中获取提现金额
                String s_withdrawAmount_new = jedis.hget("mid_licai_withdrawAmount", id);
                //在指标hash中(realtime_licai)表中获取当日历史提现金额
                String s_withdrawAmount_history = jedis.hget("realtime_licai", "withdrawAmount");
                if(s_withdrawAmount_new!=null){
                    float withdrawAmount_new = Float.parseFloat(s_withdrawAmount_new);
                    if (s_withdrawAmount_history != null) {
                        float withdrawAmount_history = Float.parseFloat(s_withdrawAmount_history);
                        //两个金额相加,继续保存到指标hash里面取
                        jedis.hset("realtime_licai", "withdrawAmount", withdrawAmount_history + withdrawAmount_new + "");
                        jedis.hset("realtime_licai", "currentDate",format.format(new Date()));
                    } else {
                        //保存到指标hash里面取
                        jedis.hset("realtime_licai", "withdrawAmount", withdrawAmount_new + "");
                        jedis.hset("realtime_licai", "currentDate",format.format(new Date()));
                    }
                }
                break;
            //08 index_08 投资金额
            case "investAmount":
                //计算逻辑:  获取到主键
                String pid = input.getStringByField("IndexValue");
                //通过这个id去中间表中获取提现金额
                String s_investAmount_new = jedis.hget("mid_licai_investAmount", pid);
                //在指标hash中(realtime_licai)表中获取当日历史提现金额
                String s_investAmount_history = jedis.hget("realtime_licai", "investAmount");
                if(s_investAmount_new!=null){
                    float investAmount_new = Float.parseFloat(s_investAmount_new);
                    if (s_investAmount_history != null) {
                        float investAmount_history = Float.parseFloat(s_investAmount_history);
                        //两个金额相加,继续保存到指标hash里面取
                        jedis.hset("realtime_licai", "investAmount", investAmount_history + investAmount_new + "");
                        jedis.hset("realtime_licai", "currentDate",format.format(new Date()));
                    } else {
                        //保存到指标hash里面取
                        jedis.hset("realtime_licai", "investAmount", investAmount_new + "");
                        jedis.hset("realtime_licai", "currentDate",format.format(new Date()));
                    }
                }
                break;
            default:
                break;
        }
        //释放掉redis连接
        connector.releaseJedis(jedis);
        //向上级确定,已经收到消息
        collector.ack(input);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
