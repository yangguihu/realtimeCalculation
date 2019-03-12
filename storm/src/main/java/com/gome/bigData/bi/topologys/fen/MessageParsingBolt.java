package com.gome.bigData.bi.topologys.fen;

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
        //jedis = new RedisConnector("10.143.90.106", 6379);
        jedis = new RedisConnector("10.143.90.38", 6379);
    }

    public void execute(Tuple input) {
        /**
         在自带KafkaSpout里面获取数据
        //获取上一级Spout发送到的字节消息数据
        byte[] binary = input.getBinary(0);
        //还原成为字符串供后面环节处理
        String value = new String(binary);
         */
        //**在自定义的KafkaSpout里面获取数据
        String value = input.getStringByField("fen_message");
        Map<String, Object> map = null;
        try {
            map = (Map<String, Object>) JSON.parse(value);
        } catch (Exception e) {
            System.err.println("消息格式不合法: 接收到 " + value + ",需要的格式为json++++++++++++++++++++++++++++");
            e.printStackTrace();
            collector.ack(input);
            return;
        }
//        String schemaName = (String) map.get("SCHEMANAME");
//        System.out.println("MessageParsingBolt Bolt接收到消息的数据库名称: " + schemaName);
//        String eventType = (String) map.get("EVENTTYPE");
//        System.out.println("MessageParsingBolt Bolt接收到消息的EventType: " + eventType);
        //Index01: 注册用户数
        countRegisterQuantity(map);
        //Index02: 鉴权人数
        countAuthenticate(map);
        //Index03: 进件件数
        countIntoPiece(map);
        //Index04: 风控件数
        countSysPass(map);
        //Index05: 电核件数
        countTelCheck(map);
        //Index06: 放款件数
        countLoanPiece(map);
        //Index07: 放款金额
        countLoanAmount(map);
        //Index08: 鉴权人数-当日注册并鉴权
        countRegisterAuthenticate(map);
        //Index09: 进件件数-当日注册并进件
        countRegisterIntoPiece(map);
        //Index10: 风控件数-当日注册并通过风控件数
        countRegisterSysPass(map);
        //Index11: 电核件数-当日注册并通过电核件数
        countRegisterTelCheck(map);
        //Index12: 放款件数-当日注册并放款件数
        countRegisterLoanPiece(map);
        //Index13: 放款金额-当日注册并放款金额
        countRegisterLoanAmount(map);
        collector.ack(input);
    }

    /**
     * 指标13: 放款金额-当日注册并放款金额
     * SQL1: SELECT
     * t.UNIQUE_ID,COMMODITY_LOAN_AMT 'loanamount_regloan_curday'
     * FROM
     * ccsdb.ccs_acct a
     * JOIN  ccsdb.ccs_merchandise_order o ON a.CONTR_NBR = o.CONTR_NBR
     * JOIN  ccsdb.ccs_merchandise_detail d ON o.MERCHANDISE_ORDER_ID = d.MERCHANDISE_ORDER_ID
     * JOIN  rmpsdb_fen.tm_app_main t ON a.APPLICATION_NO=t.APP_NO
     * AND d.COMMODITY_STATUS = 'S'
     * AND DATE_FORMAT(d.CREATE_TIME, '%Y-%m-%d') = CURDATE();
     * -----------------------------------------------------------------------------------------------------
     * SQL2:
     * SELECT
     * t.customer_id
     * FROM
     * t_customer_bank_card t
     * LEFT JOIN t_user u ON t.customer_id = u.customer_id
     * WHERE
     * t.channel = '0908'  and u.register_channel = '0908'
     * AND t.customer_id IS NOT NULL
     * AND DATE_FORMAT(u.register_date, '%Y-%m-%d') =CURRENT_DATE()
     * AND DATE_FORMAT(t.create_time, '%Y-%m-%d') =CURRENT_DATE()
     *
     * @param map
     */
    private void countRegisterLoanAmount(Map<String, Object> map) {
        //监控ccs_merchandis_detail中的Application_no
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String application_no = (String) map.get("APPLICATION_NO");
        String commodity_status = (String) map.get("COMMODITY_STATUS");
        String create_time = (String) map.get("CREATE_TIME");
        String commodity_loan_amt = (String) map.get("COMMODITY_LOAN_AMT");
        if ("ccsdb".equalsIgnoreCase(schemaname) && "ccs_merchandise_detail".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && "S".equalsIgnoreCase(commodity_status) && DateTools.isToday(create_time) && application_no != null) {
            double dcommodity_loan_amt = Double.parseDouble(commodity_loan_amt);
            //通过Application_no来查询tm_app_main中的customer_id,如果这个customer_id是今天注册的,那么
            //新算法: 在redis中有一张中间表mid_fen_current_day_application_no,保存了当日注册并通过的进件,如果ccs_merchandise_detail的进件号是在中间表中存在
            //表示此进件是当日注册并放款的指标
            Boolean flag = jedis.existsInSet("mid_fen_systemIntopieceQuantity2", application_no);//第一层验证: 查看进件号是否当日的
            String merchandise_detail_id = (String) map.get("MERCHANDISE_DETAIL_ID");//获取主键
            Boolean flag2 = jedis.existsInHash("mid_fen_loanAmount2", merchandise_detail_id);/* 第二层验证: 查看当前主键对应的金额是否是重复计数的,在mid_fen_loanAmount2临时存储主键-->金额的临时表中没有这个主键id */
            if (flag&&(!flag2)) {
                //collector.emit(new Values("loanamount_regloan_curday", commodity_loan_amt));
                HashMap<String, Object> keyValue = new HashMap<>();
                keyValue.put(merchandise_detail_id, commodity_loan_amt);
                jedis.save2hash("mid_fen_loanAmount2", keyValue);//将主键和金额保存到redis的临时表中hash数据结构里面,下一级bolt在里面
                //发送给下一级bolt一个信号,开始计算
                collector.emit(new Values("loanAmount2", merchandise_detail_id));
            }
        }
    }


    /**
     * 指标12: 放款件数-当日注册并放款件数
     * SQL1      SELECT
     * t.UNIQUE_ID,a.ACCT_NBR as acc_nbr
     * FROM
     * ccsdb.ccs_acct a
     * JOIN  ccsdb.ccs_merchandise_order o ON a.CONTR_NBR = o.CONTR_NBR
     * JOIN  ccsdb.ccs_merchandise_detail d ON o.MERCHANDISE_ORDER_ID = d.MERCHANDISE_ORDER_ID
     * JOIN  rmpsdb_fen.tm_app_main t ON a.APPLICATION_NO=t.APP_NO
     * AND d.COMMODITY_STATUS = 'S'
     * ------------------------------------------------------------------------------------------------------
     * SQL2      SELECT
     * t.customer_id
     * FROM
     * t_customer_bank_card t
     * LEFT JOIN t_user u ON t.customer_id = u.customer_id
     * WHERE
     * t.channel = '0908'  and u.register_channel = '0908'
     * AND t.customer_id IS NOT NULL
     * AND DATE_FORMAT(u.register_date, '%Y-%m-%d') =CURRENT_DATE()
     * AND DATE_FORMAT(t.create_time, '%Y-%m-%d') =CURRENT_DATE()
     * <p>
     * 计算思路2: application_no,SQL中join的目的就是为了
     *
     * @param map
     */
    private void countRegisterLoanPiece(Map<String, Object> map) {
        // 简单的做法,仅仅使用ccs_merchandise_detail这一张表,对状态为S对application_no进行计数
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String application_no = (String) map.get("APPLICATION_NO");
        String commodity_status = (String) map.get("COMMODITY_STATUS");
        String create_time = (String) map.get("CREATE_TIME");
        if ("ccsdb".equalsIgnoreCase(schemaname) && "ccs_merchandise_detail".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && "S".equalsIgnoreCase(commodity_status) && DateTools.isToday(create_time) && application_no != null) {
            Boolean flag = jedis.existsInSet("mid_fen_systemIntopieceQuantity2", application_no);//在当日注册并放款里面查进件号
            if (flag) {
                //collector.emit(new Values("user_regloanamount_curday", application_no));
                jedis.save2set("mid_fen_loanQuantity2",application_no);//将进件号保存在redis缓存里面,
                collector.emit(new Values("loanQuantity2",application_no));//发送信号给下一级bolt,然后进行计算
            }
        }
    }

    /**
     * 指标11: 电核件数-当日注册并通过电核件数
     * SQL1:     SELECT
     * t.customer_id
     * FROM
     * passport.t_customer_bank_card t
     * LEFT JOIN passport.t_user u ON t.customer_id = u.customer_id
     * WHERE
     * t.channel = '0908'  and u.register_channel = '0908'
     * AND t.customer_id IS NOT NULL
     * AND DATE_FORMAT(u.register_date, '%Y-%m-%d') = CURRENT_DATE()
     * AND DATE_FORMAT(t.create_time, '%Y-%m-%d') = CURRENT_DATE()
     * -----------------------------------------------------------------------------
     * SQL2:       SELECT
     * APP_NO,
     * UNIQUE_ID,
     * `STATUS`
     * FROM
     * tm_app_main
     * WHERE
     * DATE_FORMAT(CREATE_TIME, '%Y-%m-%d') = CURRENT_DATE()
     * AND (`STATUS` = 'J' OR `STATUS` = 'N')
     * 监控tm_app_main,检测到unique_id之后,计数
     *
     * @param map
     */
    private void countRegisterTelCheck(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String status = (String) map.get("STATUS");
        String app_no = (String) map.get("APP_NO");
        if ("rmpsdb_fen".equalsIgnoreCase(schemaname) && "tm_app_main".equalsIgnoreCase(tablename) && ("update".equalsIgnoreCase(eventType) || "insert".equalsIgnoreCase(eventType)) && app_no != null && ("j".equalsIgnoreCase(status) || "n".equalsIgnoreCase(status))) {
            String unique_id = (String) map.get("UNIQUE_ID");
            Boolean customer_id = jedis.existsInSet("mid_fen_authenticationQuantity2", unique_id);
            //String customer_id = jedis.getFromHash(2, unique_id, "customer_id");
            if (customer_id) {
                //collector.emit(new Values("user_regtelpass_curday", 1));
                jedis.save2set("mid_fen_telPassQuantity2", app_no);//
                collector.emit(new Values("telPassQuantity2", app_no));
            }
        }
    }

    /**
     * 指标10: 风控件数-当日注册并通过风控件数
     * SQL1:  SELECT t.customer_id
     * FROM
     * passport.t_customer_bank_card t
     * LEFT JOIN passport.t_user u ON t.customer_id = u.customer_id
     * WHERE
     * t.channel = '0908'  and u.register_channel = '0908'
     * AND t.customer_id IS NOT NULL
     * AND DATE_FORMAT(u.register_date, '%Y-%m-%d') = CURRENT_DATE()
     * AND DATE_FORMAT(t.create_time, '%Y-%m-%d') = CURRENT_DATE()
     * ----------------------------------------------------------------------------------
     * SQL2:      SELECT APP_NO, UNIQUE_ID, `STATUS`
     * FROM tm_app_main
     * WHERE
     * DATE_FORMAT(CREATE_TIME, '%Y-%m-%d') = CURRENT_DATE()
     * and (node_id in( 'scorecard','julixin') and (status in('J','C')) OR NODE_ID IN('TELcheck','manufraud'))
     *
     * @param map
     */
    private void countRegisterSysPass(Map<String, Object> map) {
        /*String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String create_time = (String) map.get("CREATE_TIME");
        String app_no = (String) map.get("APP_NO");
        if ("rmpsdb_fen".equalsIgnoreCase(schemaname) && "tm_app_main".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && app_no != null && DateTools.isToday(create_time)) {
            String node_id = (String) map.get("NODE_ID");
            String status = (String) map.get("STATUS");
            if (("scorecard".equalsIgnoreCase(node_id) || "juxinli".equalsIgnoreCase(node_id)) && ("J".equalsIgnoreCase(status) || "C".equalsIgnoreCase(status))) {
                //collector.emit(new Values("user_syspass_curday", 1));
                //保存进件号到redis临时表中,并发信号给下一级bolt进行计算
                jedis.save2set("mid_fen_systemPassQuantity", app_no);
                collector.emit(new Values("systemPassQuantity", app_no));
            } else if ("telcheck".equalsIgnoreCase(node_id) || "manufraud".equalsIgnoreCase(node_id)) {
                //collector.emit(new Values("user_syspass_curday", 1));
                jedis.save2set("mid_fen_systemPassQuantity", app_no);
                collector.emit(new Values("systemPassQuantity", app_no));
            }
        }*/

        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String create_time = (String) map.get("CREATE_TIME");

        String app_no = (String) map.get("APP_NO");
        if ("rmpsdb_fen".equalsIgnoreCase(schemaname) && "tm_app_main".equalsIgnoreCase(tablename) && ("update".equalsIgnoreCase(eventType) || "insert".equalsIgnoreCase(eventType)) && app_no != null && DateTools.isToday(create_time)) {
            String node_id = (String) map.get("NODE_ID");
            String status = (String) map.get("STATUS");
            String unique_id = (String) map.get("UNIQUE_ID");
            boolean flag = false;
            //String customer_id = jedis.getFromHash(2, unique_id, "customer_id");
            Boolean customer_id = jedis.existsInSet("mid_fen_authenticationQuantity2", unique_id);//判断当前进件的用户id是否在当日注册并认证临时表里面
            if (("scorecard".equalsIgnoreCase(node_id) || "juxinli".equalsIgnoreCase(node_id)) && ("J".equalsIgnoreCase(status) || "C".equalsIgnoreCase(status)) && customer_id) {
                flag = true;
            } else if ("telcheck".equalsIgnoreCase(node_id) || "manufraud".equalsIgnoreCase(node_id) && customer_id) {
                flag = true;
            }
            //0302增加限制条件<--原因,通过风控数和新增通过风控数数量一样,特增加限制
            Boolean flag2 = jedis.existsInSet("mid_fen_systemIntopieceQuantity2", app_no);//[判断当前进件号是否存在于新增进件里面
            if (flag&&flag2) {
                //collector.emit(new Values("user_regsyspass_curday", 1));
                    //保存结果到redis数据库中,
                    jedis.save2set("mid_fen_systemPassQuantity2", app_no);
                    collector.emit(new Values("systemPassQuantity2", app_no));
            }
        }
    }

    /**
     * 指标9: 新增注册并进件-当日注册并进件
     * SQL1:      SELECT APP_NO, UNIQUE_ID, STATUS
     * FROM  rmpsdb_fen.tm_app_main
     * WHERE DATE_FORMAT(CREATE_TIME, '%Y-%m-%d') = CURRENT_DATE();
     * ----------------------------------------------------------------------------------
     * SQL2:      SELECT t.customer_id as cid
     * FROM passport.t_customer_bank_card t
     * LEFT JOIN passport.t_user u ON t.customer_id = u.customer_id
     * WHERE
     * t.channel = '0908'  and u.register_channel = '0908'
     * AND t.customer_id IS NOT NULL
     * AND DATE_FORMAT(u.register_date, '%Y-%m-%d') = CURRENT_DATE()
     * AND DATE_FORMAT(t.create_time, '%Y-%m-%d') = CURRENT_DATE()
     * <p>
     * 计算逻辑: 监控进件表,有新的进件产生.查询这个进件的unique_id在
     *
     * @param map
     */
    private void countRegisterIntoPiece(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String create_time = (String) map.get("CREATE_TIME");
        String app_no = (String) map.get("APP_NO");
        if ("rmpsdb_fen".equalsIgnoreCase(schemaname) && "tm_app_main".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && app_no != null && DateTools.isToday(create_time)) {
            String unique_id = (String) map.get("UNIQUE_ID");
            if(unique_id!=null){
                Boolean flag = jedis.existsInSet("mid_fen_authenticationQuantity2", unique_id);//判断当前进件的用户id是否在当日注册并认证的id里面
                if (flag) {
                    //保存数据到redis中间表中
                    jedis.save2set("mid_fen_systemIntopieceQuantity2", app_no);
                    collector.emit(new Values("systemIntopieceQuantity2", app_no));
                }
            }
        }
    }

    /**
     * 指标8: 新增注册并鉴权
     * SELECT COUNT(t.customer_id)
     * FROM
     * t_customer_bank_card t
     * LEFT JOIN t_user u on t.customer_id=u.customer_id
     * WHERE
     * t.channel='0908'  and u.register_channel = '0908'
     * AND t.customer_id is not null
     * AND DATE_FORMAT(u.register_date,'%Y-%m-%d')=CURRENT_DATE()
     * AND DATE_FORMAT(t.create_time,'%Y-%m-%d')=CURRENT_DATE();
     *
     * @param map
     */
    private void countRegisterAuthenticate(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String channel = (String) map.get("channel");
        String create_time = (String) map.get("create_time");//t_customer_bank_card表的create_time,即认证时间
        String customer_id = (String) map.get("customer_id");

        if ("passport".equalsIgnoreCase(schemaname) && "t_customer_bank_card".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && "0908".equalsIgnoreCase(channel) && customer_id != null && DateTools.isToday(create_time)) {
            //查找t_user表中相应的customer_id所在记录中的register_time,如果也是今天,那么这个用户就是当日注册并认证的
            Timestamp passport = (Timestamp) queryRunner.queryObject("passport", "select register_date from t_user where customer_id=?", "register_date", customer_id);
            String t_user_register_date = DateTools.getStringDate(passport);
            boolean sameDate = DateTools.isSameDate(create_time, t_user_register_date);//比较两个时间是否是同一天 1,认证创建时间;2,查数据库得到的注册时间
            //经过判断,如果是当天注册,并当天认证,那么就是合法数据,发送给下一级做判断,并且,保存一份数据到redis作为中间数据
            if (sameDate) {
                jedis.save2set("mid_fen_authenticationQuantity2", customer_id);//当日注册并认证的用户的customer_id
                collector.emit(new Values("authenticationQuantity2", customer_id));//给下一级发送信号,去计算
            }
        }
    }

    /**
     * 指标7: 放款金额
     * 持久化字段: loanamount_curday
     * 计算逻辑: 进入rmpsdb_fen.tm_app_main表的MERCHANDISE_ORDER_STATUS = 'Y'且APPLICATION_NO=ccsdb.CCS_MERCHANDISE_ORDER中的app_no,则计数
     * SELECT
     * SUM(CASE WHEN DATE_FORMAT(d.CREATE_TIME, '%Y-%m-%d') = CURDATE() THEN COMMODITY_LOAN_AMT ELSE 0 END) AS 'loanamount_curday'
     * FROM
     * ccsdb.ccs_acct a
     * JOIN  ccsdb.ccs_merchandise_order o ON a.CONTR_NBR = o.CONTR_NBR
     * JOIN  ccsdb.ccs_merchandise_detail d ON o.MERCHANDISE_ORDER_ID = d.MERCHANDISE_ORDER_ID
     * JOIN  rmpsdb_fen.tm_app_main t ON a.APPLICATION_NO=t.APP_NO
     * AND d.COMMODITY_STATUS = 'S'
     *
     * @param map
     */
    private void countLoanAmount(Map<String, Object> map) {
        // 简单的做法,仅仅使用ccs_merchandise_detail这一张表,对状态为S对application_no进行计数
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String application_no = (String) map.get("APPLICATION_NO");
        String commodity_status = (String) map.get("COMMODITY_STATUS");
        String create_time = (String) map.get("CREATE_TIME");
        String commodity_loan_amt = (String) map.get("COMMODITY_LOAN_AMT");

        if ("ccsdb".equalsIgnoreCase(schemaname) && "ccs_merchandise_detail".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && "S".equalsIgnoreCase(commodity_status) && DateTools.isToday(create_time) && commodity_loan_amt != null) {
            //double dcommodity_loan_amt = Double.parseDouble(commodity_loan_amt);
            //collector.emit(new Values("loanamount_curday", dcommodity_loan_amt));
            //提取主键,和放款金额,保存到redis临时表中
            String merchandise_detail_id = (String) map.get("MERCHANDISE_DETAIL_ID");
            //临时表中不存在merchandise_detail_id的情况下,允许插入新主键对应的数据到hash结构里面
            Boolean flag = jedis.existsInHash("mid_fen_loanAmount", merchandise_detail_id);
            if (!flag) {
                HashMap<String, Object> keyValue = new HashMap<>();
                keyValue.put(merchandise_detail_id, commodity_loan_amt);
                jedis.save2hash("mid_fen_loanAmount", keyValue);
                //发送给下一级bolt一个信号,开始计算
                collector.emit(new Values("loanAmount", merchandise_detail_id));
            }

        }
    }
    /*private void countLoanAmount(Map<String, Object> map) {
        //0,获取
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");

        String commodity_status = (String) map.get("COMMODITY_STATUS");
        //String application_no = (String) map.get("APPLICATION_NO");
        if ("ccsdb".equalsIgnoreCase(schemaname) && "ccs_merchandise_detail".equalsIgnoreCase(tablename) && "INSERT".equalsIgnoreCase(eventType) && "S".equalsIgnoreCase(commodity_status)) {
            long commodity_loan_amt = (long) map.get("COMMODITY_LOAN_AMT");
            collector.emit(new Values("loanamount_curday", commodity_loan_amt));
            *//*long commodity_loan_amt = (long) map.get("commodity_loan_amt");
            //1,通过消息中的contr_nbr查询出MERCHANDISE_ORDER_ID
            String merchandise_order_id = queryRunner.query("ccsdb", "select merchandise_order_id from ccs_merchandise_order where contr_nbr=?", "merchandise_order_id", String.class, contr_nbr);
            //2,通过上一级查出的 merchandise_order_id 查询 commodity_status
            String commodity_status = null;
            if (merchandise_order_id != null) {
                commodity_status = queryRunner.query("ccsdb", "select commodity_status from ccs_merchandise_detail where merchandise_order_id=?", "commodity_status", String.class, merchandise_order_id);
            }
            //3,通过消息中获取到的 application_no 查询rmpsdb_fen.tm_app_main表中是否有app_no
            Boolean exists = queryRunner.exists("rmpsdb_fen", "select app_no from tm_app_main where app_no=?", "app_no", application_no);
            //4,上面条件全部符合,则发送数据给下一级
            if ("s".equalsIgnoreCase(commodity_status) && exists) {
                collector.emit(new Values("loanamount_curday", commodity_loan_amt));
            }*//*
        }
    }*/

    /**
     * 指标6: 放款件数
     * 持久化字段: user_loanamount_curday
     * 计算逻辑:
     * SELECT
     * COUNT(DISTINCT a.ACCT_NBR) as acc_nbr
     * FROM
     * ccsdb.ccs_acct a
     * JOIN  ccsdb.ccs_merchandise_order o ON a.CONTR_NBR = o.CONTR_NBR                             <--通过消息中的contr_nbr查询出merchandise_order_id
     * JOIN  ccsdb.ccs_merchandise_detail d ON o.MERCHANDISE_ORDER_ID = d.MERCHANDISE_ORDER_ID      <--通过上一级查出的 merchandise_order_id 查询 commodity_status
     * JOIN  rmpsdb_fen.tm_app_main t ON a.APPLICATION_NO=t.APP_NO                                  <--通过消息中获取到的 application_no 查询rmpsdb_fen.tm_app_main表中是否有app_no
     * AND d.COMMODITY_STATUS = 'S'
     * AND DATE_FORMAT(d.CREATE_TIME, '%Y-%m-%d') = CURRENT_DATE();
     * <p>
     * // SELECT COUNT(DISTINCT application_no) FROM ccs_merchandise_detail WHERE COMMODITY_STATUS='S' AND DATE_FORMAT(CREATE_TIME, '%Y-%m-%d') = CURRENT_DATE();
     *
     * @param map
     */
    private void countLoanPiece(Map<String, Object> map) {
        // 简单的做法,仅仅使用ccs_merchandise_detail这一张表,对状态为S对application_no进行计数
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String application_no = (String) map.get("APPLICATION_NO");
        String commodity_status = (String) map.get("COMMODITY_STATUS");
        String create_time = (String) map.get("CREATE_TIME");
        if ("ccsdb".equalsIgnoreCase(schemaname) && "ccs_merchandise_detail".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && "S".equalsIgnoreCase(commodity_status) && DateTools.isToday(create_time) && application_no != null) {
            //collector.emit(new Values("user_loanamount_curday", application_no));
            //保存进件号到redis临时表中,发送信号给下一级bolt进行计算
            jedis.save2set("mid_fen_loanQuantity", application_no);
            collector.emit(new Values("loanQuantity", application_no));
        }
    }

    /**
     * 指标5: 电核通过件数
     * 持久化字段: user_telpass_curday
     * 计算逻辑: 进入rmpsdb_fen.tm_app_main表的Status为N或者为J则计数
     * select
     * sum(case when Status = 'N' or Status='J' then 1 else 0 end) as sp_num
     * from
     * TM_APP_MAIN   t
     * WHERE
     * DATE_FORMAT(CREATE_TIME, '%Y-%m-%d') = CURRENT_DATE();
     * 监控状态,如果有一个状态为N或者J的进件,则表示电核通过
     *
     * @param map
     */
    private void countTelCheck(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String create_time = (String) map.get("CREATE_TIME");
        String app_no = (String) map.get("APP_NO");
        if ("rmpsdb_fen".equalsIgnoreCase(schemaname) && "tm_app_main".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && DateTools.isToday(create_time)) {
            String status = (String) map.get("STATUS");
            if (("n".equalsIgnoreCase(status) || "j".equalsIgnoreCase(status))) {
                //collector.emit(new Values("user_telpass_curday", 1));
                //保存数据到jedis临时表中,并发送信号给下一级进行去重计算
                jedis.save2set("mid_fen_telPassQuantity", app_no);
                collector.emit(new Values("telPassQuantity", app_no));
            }
        }
    }

    /**
     * 指标4: 风控通过件数
     * 持久化字段: user_syspass_curday
     * 计算逻辑: 进入rmpsdb_fen.tm_app_main表的app_no
     * SELECT count(app_no) FROM
     * tm_app_main
     * WHERE DATE_FORMAT(CREATE_TIME, '%Y-%m-%d') = CURRENT_DATE()
     * AND((node_id IN ('scorecard', 'juxinli')AND STATUS IN ('J', 'C')) OR NODE_ID IN ('telcheck', 'manufraud'))
     *
     *#--------------------------修改后逻辑---------------------
     * SELECT COUNT(*)
     *FROM rule_output_bigdata
     *WHERE DATE(CREATE_TIME)='2017-03-10' AND APPROVAL_RESULT in('TELCHECK')
     * @param map
     */
    private void countSysPass(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String create_time = (String) map.get("CREATE_TIME");
        String app_no = (String) map.get("APP_NO");
        //if ("rmpsdb_fen".equalsIgnoreCase(schemaname) && "rule_output_bigdata".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && app_no != null && DateTools.isToday(create_time)) {
        if (schemaname!=null
                && tablename !=null
                && eventType !=null
                && app_no != null
                && create_time !=null
                && "rmpsdb_fen".equalsIgnoreCase(schemaname)
                && "rule_output_bigdata".equalsIgnoreCase(tablename)
                && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType))
                && DateTools.isToday(create_time)) {
            String approval_result =(String) map.get("APPROVAL_RESULT");
            if("TELCHECK".equals(approval_result)){
                jedis.save2set("mid_fen_systemPassQuantity", app_no);
                collector.emit(new Values("systemPassQuantity", app_no));
            }
        }
    }

    /**
     * 指标3: 进件件数
     * 持久化字段: user_intopiece_curday
     * 计算逻辑: 进入rmpsdb_fen.tm_app_main表的app_no
     * select
     * count(t.app_no)
     * from tm_app_main  t
     * where DATE_FORMAT(t.create_time,'%Y-%m-%d')=CURRENT_DATE();
     *
     * @param map
     */
    private void countIntoPiece(Map<String, Object> map) {
        //01,当日app_no不为null就计数
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventType = (String) map.get("EVENTTYPE");
        String create_time = (String) map.get("CREATE_TIME");
        String app_no = (String) map.get("APP_NO");
        if ("rmpsdb_fen".equalsIgnoreCase(schemaname) && "tm_app_main".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) && app_no != null && DateTools.isToday(create_time)) {
            //collector.emit(new Values("user_intopiece_curday", 1));
            jedis.save2set("mid_fen_systemIntopieceQuantity", app_no);
            collector.emit(new Values("systemIntopieceQuantity", app_no));
        }
    }

    /**
     * 指标2:　鉴权人数
     * 持久化字段: user_auth_curday
     * 定义：业务流程上指的是填写完身份认证信息（填写完 身份证、银行卡、预留手机号等）。当日注册并认证的
     * SELECT COUNT(distinct t.customer_id)
     * FROM
     * t_customer_bank_card t
     * LEFT JOIN
     * t_user u on t.customer_id=u.customer_id
     * WHERE
     * t.channel='0908'
     * AND t.customer_id is not null
     * AND DATE_FORMAT(t.create_time,'%Y-%m-%d')=CURRENT_DATE();
     * 计算逻辑:
     * 1,监控t_customer_bank_card表,获取customer_id字段
     * 2,通过customer_id字段去t_user表里面查找是否存在该用户(可能不是当日注册并鉴权,所以,需要查mysql数据库找t_user)
     *
     * @param map
     */
    private void countAuthenticate(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String channel = (String) map.get("channel");
        String create_time = (String) map.get("create_time");

        if ("passport".equalsIgnoreCase(schemaname) && "t_customer_bank_card".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && "0908".equalsIgnoreCase(channel) && DateTools.isToday(create_time)) {
            //获取t_customer_bank_card表的custormer_id
            String customer_id = (String) map.get("customer_id");
            //将结果保存到redis中间表中 redis临时key名: mid_fen_userAuthenticationQuantity
            jedis.save2set("mid_fen_authenticationQuantity", customer_id);
            //发送信号给下一级bolt,计算总数
            collector.emit(new Values("authenticationQuantity", customer_id));

            //2,查询t_user表,如果存在于t_user表,则数据有效,发送给下一级
            //Boolean flag = queryRunner.exists("passport", "select customer_id from t_user where customer_id=?", "customer_id", customer_id);
            //if (flag) {
            //   collector.emit(new Values("user_auth_curday", 1));
            //}
        }
    }

    /**
     * 指标1: 当日注册用户数
     * 持久化字段: user_register_curday
     * 监控表: t_user
     * SQL: SELECT COUNT(1)
     * FROM t_user
     * WHERE register_channel='0908' AND DATE_FORMAT(register_date,'%Y-%m-%d')= CURRENT_DATE();
     *
     * @param map
     */
    private void countRegisterQuantity(Map<String, Object> map) {
        String schemaname = (String) map.get("SCHEMANAME");
        String tablename = (String) map.get("TABLENAME");
        String eventtype = (String) map.get("EVENTTYPE");
        String register_channel = (String) map.get("register_channel");
        String register_date = (String) map.get("register_date");
        String id = (String) map.get("id");

        if ("passport".equalsIgnoreCase(schemaname) && "t_user".equalsIgnoreCase(tablename) && ("INSERT".equalsIgnoreCase(eventtype) || "UPDATE".equalsIgnoreCase(eventtype)) && "0908".equalsIgnoreCase(register_channel) && DateTools.isToday(register_date)) {
            //计算逻辑,保存主键号到redis临时表中: redis临时key名: mid_fen_userRegisterQuantity
            jedis.save2set("mid_fen_registerQuantity", id);
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
