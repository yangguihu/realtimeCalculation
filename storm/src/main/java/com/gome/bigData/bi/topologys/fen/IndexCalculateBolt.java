package com.gome.bigData.bi.topologys.fen;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.gome.bigData.bi.db.mysql.DBUtils;
import com.gome.bigData.bi.db.redis.RedisConnector;
import com.gome.bigData.bi.db.redis.RedisUtils;
import com.gome.bigData.bi.tools.DateTools;
import redis.clients.jedis.Jedis;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by MaLi on 2017/1/5.
 *
 *
 */
public class IndexCalculateBolt implements IRichBolt{
    private Map conf = null;
    private TopologyContext context = null;
    private OutputCollector collector = null;
    // TODO: 2017/3/1  config1
    //private RedisConnector connector = new RedisConnector("10.143.90.106", 6379);
    private RedisConnector connector = new RedisConnector("10.143.90.38", 6379);

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.conf = stormConf;
        this.context = context;
        this.collector = collector;
    }

    public void execute(Tuple input) {
        //***** 获取Jedis连接,准备保存指标名称和指标值到Hash数据结构 key: realtime_fen   value: 指标名称:指标值
        Jedis jedis = connector.getJedis();
        //01 判断是否需要清空redis数据,提取时间字段,
        String current_date_in_redis = connector.getFromHash("realtime_fen", "currentDate");//全格式时间
        if(!DateTools.isToday(current_date_in_redis)){//如果和今天不等,那么清空数据库
            //01保存数据到mysql数据库
            /*DataSource canal_test = DBUtils.getDataSource("canal_test");//保存数据到关系数据库里面
            Connection connection = null;
            PreparedStatement statement = null;
            try {
                //1,提取redis中realtime_index_fen中的指标数据
                //2,保存数据到mysql
                connection = canal_test.getConnection();
                String sql_saveIndex = "INSERT INTO realtime_index_fen (current_dates, user_register_curday,user_auth_curday,user_intopiece_curday,user_syspass_curday,user_telpass_curday,user_loanamount_curday,loanamount_curday, user_regauth_curday,user_regintopiece_curday,user_regsyspass_curday,user_regtelpass_curday,user_regloanamount_curday,loanamount_regloan_curday) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                statement = connection.prepareStatement(sql_saveIndex);
                statement.setString(1,jedis.hget("realtime_fen","currentDate"));
                statement.setString(2,jedis.hget("realtime_fen","registerQuantity"));
                statement.setString(3,jedis.hget("realtime_fen","authenticationQuantity"));
                statement.setString(4,jedis.hget("realtime_fen","systemIntopieceQuantity"));
                statement.setString(5,jedis.hget("realtime_fen","systemPassQuantity"));
                statement.setString(6,jedis.hget("realtime_fen","telPassQuantity"));
                statement.setString(7,jedis.hget("realtime_fen","loanQuantity"));
                statement.setString(8,jedis.hget("realtime_fen","loanAmount"));
                statement.setString(9,jedis.hget("realtime_fen","authenticationQuantity2"));
                statement.setString(10,jedis.hget("realtime_fen","systemIntopieceQuantity2"));
                statement.setString(11,jedis.hget("realtime_fen","systemPassQuantity2"));
                statement.setString(12,jedis.hget("realtime_fen","telPassQuantity2"));
                statement.setString(13,jedis.hget("realtime_fen","loanQuantity2"));
                statement.setString(14,jedis.hget("realtime_fen","loanAmount2"));
                statement.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }finally {
                DBUtils.release(connection,statement,null);
            }*/
            //02清空redis 0#数据库中的美易分指标库,多个临时中间表
            ArrayList<String> keys = new ArrayList<>();
            keys.add("realtime_fen");
            keys.add("mid_fen_registerQuantity");
            keys.add("mid_fen_authenticationQuantity");
            keys.add("mid_fen_systemIntopieceQuantity");
            keys.add("mid_fen_systemPassQuantity");
            keys.add("mid_fen_telPassQuantity");
            keys.add("mid_fen_loanQuantity");
            keys.add("mid_fen_loanAmount");
            keys.add("mid_fen_authenticationQuantity2");
            keys.add("mid_fen_systemIntopieceQuantity2");
            keys.add("mid_fen_systemPassQuantity2");
            keys.add("mid_fen_telPassQuantity2");
            keys.add("mid_fen_loanQuantity2");
            keys.add("mid_fen_loanAmount2");

            connector.deleteKeys(keys);
        }

        //01 获取上一级MessageParsingBolt发送过来的指标
        String indexName = input.getStringByField("IndexName");//美易分产品线指标名称
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        //准备待插入数据
        HashMap<String, String> map = new HashMap<>();
        map.put("currentDate",format.format(new Date()));
        //目的创建key: realtime_index_fen
        if(!jedis.exists("realtime_fen"))
            jedis.hset("realtime_fen","currentDate",format.format(new Date()));
        //根据不同的指标分别落库
        switch (indexName){
            //Index1: registerQuantity 注册用户数
            case "registerQuantity":
                //去中间表算t_user表里面主键的总个数
                Long fen_registerQuantity = jedis.scard("mid_fen_registerQuantity");
                map.put("registerQuantity",fen_registerQuantity+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;
            //Index2: authenticationQuantity 鉴权用户数
            case "authenticationQuantity":
                //去中间表算t_customer_bank_card表里面customer_id的总个数
                Long fen_authenticationQuantity = jedis.scard("mid_fen_authenticationQuantity");
                map.put("authenticationQuantity",fen_authenticationQuantity+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;
            //Index3: systemIntopieceQuantity 进件数
            case "systemIntopieceQuantity":
                Long mid_fen_systemIntopieceQuantity = jedis.scard("mid_fen_systemIntopieceQuantity");
                map.put("systemIntopieceQuantity",mid_fen_systemIntopieceQuantity+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;
            //Index4: systemPassQuantity 进入风控数
            case "systemPassQuantity":
                Long mid_fen_systemPassQuantity = jedis.scard("mid_fen_systemPassQuantity");
                map.put("systemPassQuantity",mid_fen_systemPassQuantity+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;
            //Index5: telPassQuantity 电核数
            case "telPassQuantity":
                Long mid_fen_telPassQuantity = jedis.scard("mid_fen_telPassQuantity");
                map.put("telPassQuantity",mid_fen_telPassQuantity+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;

            //Index6: loanQuantity 放款数
            //将application_no保存到0#数据库,自动去重,然后计算个数,保存到redis的realtime_index_fen的user_loanamount_curday字段里面
            case "loanQuantity":
                Long mid_fen_loanQuantity = jedis.scard("mid_fen_loanQuantity");
                map.put("loanQuantity",mid_fen_loanQuantity+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;

            /*case "user_loanamount_curday":
                String user_loanamount_curday = jedis.hget("realtime_index_fen", "user_loanamount_curday");
                if(user_loanamount_curday == null){
                    HashMap<String, String> map = new HashMap<>();
                    map.put("current_date",format.format(new Date()));
                    map.put("user_loanamount_curday",1+"");
                    jedis.hmset("realtime_index_fen",map);
                }else{
                    //如果没有user_register_curday字段的值
                    jedis.hincrBy("realtime_index_fen","user_loanamount_curday",1);
                }
                break;*/
            //Index7: loanamount_curday 放款金额
            //先取后存  mid_fen_loanAmount
            case "loanAmount":
                //首先获取数据库中存取的历史结果投资金额loanamount_curday
                String loanamount_curday = jedis.hget("realtime_fen", "loanAmount");
                //获取中间表中保存的新发送的新的放款金额的进件的id,这个值是去重之后发送过来的,只能被处理一次,直接去中间表里面读取
                String merchandise_detail_id = input.getStringByField("IndexValue");
                String mid_fen_loanAmount = jedis.hget("mid_fen_loanAmount", merchandise_detail_id);
                if(loanamount_curday!=null){
                    double historyAmount = Double.parseDouble(loanamount_curday);
                    double newAmount = Double.parseDouble(mid_fen_loanAmount);
                    map.put("loanAmount",historyAmount+newAmount+"");
                    //保存最终数据到结果中
                    jedis.hmset("realtime_fen",map);
                }else{
                    double newAmount = Double.parseDouble(mid_fen_loanAmount);
                    map.put("loanAmount",newAmount+"");
                    //保存最终数据到结果中
                    jedis.hmset("realtime_fen",map);
                }
                break;

            //Index8: authenticationQuantity2 当日注册并鉴权用户数
            case "authenticationQuantity2":
                //在中间表中计数 mid_fen_authenticationQuantity2
                Long mid_fen_authenticationQuantity2 = jedis.scard("mid_fen_authenticationQuantity2");
                map.put("authenticationQuantity2",mid_fen_authenticationQuantity2+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;

            //Index9: systemIntopieceQuantity2 当日注册并进件用户数
            case "systemIntopieceQuantity2":
                //在中间表中计数 mid_fen_authenticationQuantity2
                Long mid_fen_systemIntopieceQuantity2 = jedis.scard("mid_fen_systemIntopieceQuantity2");
                map.put("systemIntopieceQuantity2",mid_fen_systemIntopieceQuantity2+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;

            //Index10: mid_fen_systemPassQuantity2 当日注册并通过风控用户数
            case "systemPassQuantity2":
                //在中间表中计数 mid_fen_authenticationQuantity2
                Long mid_fen_systemPassQuantity2 = jedis.scard("mid_fen_systemPassQuantity2");
                map.put("systemPassQuantity2",mid_fen_systemPassQuantity2+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;

            //Index11: user_auth_curday 当日注册并通过电核用户数
            case "telPassQuantity2":
                //在中间表中计数 mid_fen_authenticationQuantity2
                Long mid_fen_telPassQuantity2 = jedis.scard("mid_fen_telPassQuantity2");
                map.put("telPassQuantity2",mid_fen_telPassQuantity2+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;

            //Index12: loanQuantity2 当日注册并放款件数
            case "loanQuantity2":
                //在中间表中计数 mid_fen_loanQuantity2
                Long mid_fen_loanQuantity2 = jedis.scard("mid_fen_loanQuantity2");
                map.put("loanQuantity2",mid_fen_loanQuantity2+"");
                //将数据保存到realtime_fen表里面
                jedis.hmset("realtime_fen",map);
                break;

            //Index13: loanAmount2 当日注册并放款金额
            case "loanAmount2":
                //首先获取数据库中存取的历史结果投资金额loanamount_curday
                String loanAmount2 = jedis.hget("realtime_fen", "loanAmount2");
                //获取中间表中保存的新发送的新的放款金额的进件的id,这个值是去重之后发送过来的,只能被处理一次,直接去中间表里面读取
                String merchandise_detail_id2 = input.getStringByField("IndexValue");
                String mid_fen_loanAmount2 = jedis.hget("mid_fen_loanAmount2", merchandise_detail_id2);
                if(loanAmount2!=null){
                    double historyAmount2 = Double.parseDouble(loanAmount2);
                    double newAmount2 = Double.parseDouble(mid_fen_loanAmount2);
                    map.put("loanAmount2",historyAmount2+newAmount2+"");
                    //保存最终数据到结果中
                    jedis.hmset("realtime_fen",map);
                }else{
                    double newAmount2 = Double.parseDouble(mid_fen_loanAmount2);
                    map.put("loanAmount2",newAmount2+"");
                    //保存最终数据到结果中
                    jedis.hmset("realtime_fen",map);
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
