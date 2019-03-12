package com.gome.bigData.bi.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.gome.bigData.bi.common.Configs;
import com.gome.bigData.bi.kafka.utils.KafkaProducerUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by MaLi on 2017/1/3.
 * 解析Canal监控到的变化,并将数据组织成为json串发送出去
 */
public class DataSender_Fen_11_3307 {
    private static int getNum = 0;

    public static void main(String args[]) {
        final String zkServers = Configs.get("zkServers");
        String destination = Configs.get("DataSender_Fen_11_3307.destination");
        CanalConnector connector = CanalConnectors.newClusterConnector(zkServers, destination, "", "");
        int batchSize = Integer.parseInt(Configs.get("batchSize"));//一次获取多少数据过来
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe("passport\\.t_user,passport\\.t_customer_bank_card");
            connector.rollback();
            int totalEmptyCount = Integer.parseInt(Configs.get("DataSender_Fen_11_3307.totalEmptyCount"));//控制无数据导致的空请求次数
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据,Message里面封装了一个List<Entry>
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    //System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    emptyCount = 0;
                    //解析获取到的消息,这个消息本质上是里面封装了一个List<Entry>
                    sendData(message.getEntries());
                }
                connector.ack(batchId); // 确认收到消息
                // connector.rollback(batchId); // 处理失败, 回滚数据
                //System.err.println("Num:2  ***************************总计收到消息: " + getNum + " 条.....");
            }
            //System.out.println("empty too many times, exit");
        } finally {
            //结束的时候释放掉到canal_server的链接
            connector.disconnect();
        }
    }

    /**
     * 解析实体,实体代表的是一个具体的事件: event
     *
     * @param entrys
     */
    private static void sendData(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            //解析到事务语法,跳过不做处理
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }
            //
            CanalEntry.EventType eventType = rowChage.getEventType();
//            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
//                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
//                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
//                    eventType));
            //获取数据库名,表名,字段名,字段值
            //实体的Header里面存的是元数据
            LinkedHashMap<String, Object> metaMap = new LinkedHashMap<String, Object>();
            metaMap.put("SCHEMANAME", entry.getHeader().getSchemaName());   //数据库名
            metaMap.put("TABLENAME", entry.getHeader().getTableName());     //表名
            metaMap.put("EVENTTYPE", eventType);                            //数据记录操作类型: 关注insert和update操作

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                getNum++;
                if (eventType == CanalEntry.EventType.DELETE) {
                    getFields(rowData.getBeforeColumnsList(), metaMap);
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    getFields(rowData.getAfterColumnsList(), metaMap);
                } else {
                    getFields(rowData.getAfterColumnsList(), metaMap);
                }
            }
            //将map转为json串
            String message = JSON.toJSONString(metaMap);
            //将数据发送到指定topic
            String topicName = Configs.get("DataSender_Fen_11_3307.topicName");
            KafkaProducerUtils.send(topicName, null, message);
        }
    }

    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private static void getFields(List<CanalEntry.Column> columns, Map<String, Object> metaMap) {
        for (CanalEntry.Column column : columns) {
            //System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            metaMap.put(column.getName(), column.getValue());
        }
    }
}
