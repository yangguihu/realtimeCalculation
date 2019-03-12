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
 * Created by MaLi on 2017/2/26.
 */
public class DataSender_Licai_11_3306 {
    private static int getNum=0;
    public static void main(String args[]) {
        String zkServers = Configs.get("zkServers");
        String destination = Configs.get("DataSender_Licai_11_3306.destination");
        CanalConnector connector = CanalConnectors.newClusterConnector(zkServers, destination, "", "");
        int batchSize = Integer.parseInt(Configs.get("batchSize"));
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe("Biz\\.TB_USER,Biz\\.TB_FUND_RECORD,Biz\\.TB_INVEST");//不配置的情况即监控instance.properties
            //connector.subscribe(".*\\..*");//监控数据库下所有库中所有表
            connector.rollback();
            int totalEmptyCount = Integer.parseInt(Configs.get("DataSender_Licai_11_3306.totalEmptyCount"));//控制无数据导致的空请求次数
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据,Message里面封装了一个List<Entry>
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    sendData(message.getEntries());//解析Entry发送消息到kafka
                }
                connector.ack(batchId); // 确认收到消息
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
            //System.out.println("empty too many times, exit");//失败多次会跳出当前进程
        } finally {
            connector.disconnect();
        }
    }
    /**
     * 解析实体,实体代表的是一个具体的事件: event
     * @param entrys
     */
    private static void sendData(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),e);
            }
            CanalEntry.EventType eventType = rowChage.getEventType();
//            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
//                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
//                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
//                    eventType));
            //获取数据库名,表名,字段名,字段值
            LinkedHashMap<String, Object> metaMap = new LinkedHashMap<String,Object>();
            metaMap.put("SCHEMANAME",entry.getHeader().getSchemaName());
            metaMap.put("TABLENAME",entry.getHeader().getTableName());
            metaMap.put("EVENTTYPE",eventType);

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                getNum++;
                if (eventType == CanalEntry.EventType.DELETE) {
                    getFields(rowData.getBeforeColumnsList(),metaMap);
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    getFields(rowData.getAfterColumnsList(),metaMap);
                } else {
                    getFields(rowData.getAfterColumnsList(),metaMap);
                }
            }
            //将map转为json
            String message = JSON.toJSONString(metaMap);
            //将数据发送到指定topic
            KafkaProducerUtils.send(Configs.get("DataSender_Licai_11_3306.topicName"),null,message);
        }
    }
    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
    private static void getFields(List<CanalEntry.Column> columns,Map<String,Object> metaMap){
        for (CanalEntry.Column column : columns) {
            //System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            metaMap.put(column.getName(),column.getValue());
        }
    }
}
